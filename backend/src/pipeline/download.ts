/**
 * Download 實價登錄 open data CSVs from 內政部
 * Source: https://plvr.land.moi.gov.tw/DownloadOpenData
 *
 * The API serves ZIP files containing CSVs for each city/season.
 * URL pattern: GET with city code + data type + season params.
 */

import { mkdirSync, createWriteStream, existsSync, unlinkSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import path from "node:path";
import { CITY_CODES } from "../types.js";

const MAX_RETRIES = 3;
const RETRY_BASE_MS = 2000;

/** Fetch with exponential backoff; retries on transient 5xx or network errors. */
async function fetchWithRetry(
  url: string,
  headers: Record<string, string>,
  attempt = 0
): Promise<Response> {
  try {
    const res = await fetch(url, {
      headers,
      // Fresh signal per attempt — reusing a signal across retries can
      // cause retries to abort immediately if the original timer elapsed.
      signal: AbortSignal.timeout(60_000),
    });
    // Only retry transient server errors (5xx); 4xx are permanent
    if (res.status >= 500 && attempt < MAX_RETRIES) {
      const delay = RETRY_BASE_MS * Math.pow(2, attempt);
      console.warn(`[retry] HTTP ${res.status} — waiting ${delay}ms before retry ${attempt + 1}/${MAX_RETRIES}`);
      await new Promise(r => setTimeout(r, delay));
      return fetchWithRetry(url, headers, attempt + 1);
    }
    return res;
  } catch (err) {
    if (attempt < MAX_RETRIES) {
      const delay = RETRY_BASE_MS * Math.pow(2, attempt);
      console.warn(`[retry] Network error (${(err as Error).message}) — waiting ${delay}ms before retry ${attempt + 1}/${MAX_RETRIES}`);
      await new Promise(r => setTimeout(r, delay));
      return fetchWithRetry(url, headers, attempt + 1);
    }
    throw err;
  }
}

const DATA_DIR = path.resolve(import.meta.dirname, "../../../data/downloads");

// 內政部 open data download URL
// Type 0 = 不動產買賣 (real estate sales)
// Type 1 = 預售屋買賣
// Type 2 = 不動產租賃
const BASE_URL = "https://plvr.land.moi.gov.tw/DownloadSeason";

interface DownloadOptions {
  /** ROC year, e.g. 113 for 2024 */
  year: number;
  /** Season 1-4 */
  season: number;
  /** City codes to download (default: all) */
  cities?: string[];
  /** Data type: 0=sales, 1=presale, 2=rental */
  type?: number;
}

export async function downloadPlvr(options: DownloadOptions): Promise<string[]> {
  const { year, season, cities, type = 0 } = options;
  const cityCodes = cities ?? Object.keys(CITY_CODES);

  mkdirSync(DATA_DIR, { recursive: true });

  const downloaded: string[] = [];

  for (const cityCode of cityCodes) {
    const filename = `${cityCode}_lvr_land_${type === 0 ? "A" : type === 1 ? "B" : "C"}_${year}S${season}.csv`;
    const outPath = path.join(DATA_DIR, filename);

    if (existsSync(outPath)) {
      console.log(`[skip] ${filename} already exists`);
      downloaded.push(outPath);
      continue;
    }

    // The API expects query params for season/city selection
    const params = new URLSearchParams({
      fileName: `${cityCode}_lvr_land_${type === 0 ? "A" : type === 1 ? "B" : "C"}`,
      season: `${year}S${season}`,
      type: "csv",
    });

    const url = `${BASE_URL}?${params.toString()}`;
    console.log(`[download] ${cityCode} (${CITY_CODES[cityCode]}) ${year}S${season}...`);

    const HEADERS = {
      "User-Agent": "RealEstateRadar/0.1 (github.com/copilot-autogent/realestate-radar)",
      Accept: "text/csv,application/octet-stream,*/*",
    };

    try {
      const res = await fetchWithRetry(url, HEADERS);

      if (!res.ok) {
        console.warn(`[warn] ${cityCode}: HTTP ${res.status} after retries — skipping`);
        continue;
      }

      if (!res.body) {
        console.warn(`[warn] ${cityCode}: empty response body — skipping`);
        continue;
      }

      // Write to a temp path first; rename on success to avoid partial files
      const tmpPath = `${outPath}.tmp`;
      const writable = createWriteStream(tmpPath);
      try {
        await pipeline(Readable.fromWeb(res.body as any), writable);
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const { renameSync } = await import("node:fs");
        renameSync(tmpPath, outPath);
      } catch (writeErr) {
        // Clean up partial file
        try { unlinkSync(tmpPath); } catch { /* ignore */ }
        throw writeErr;
      }
      console.log(`[ok] ${filename}`);
      downloaded.push(outPath);
    } catch (err) {
      console.error(`[error] ${cityCode}:`, (err as Error).message);
      // Non-fatal: pipeline continues with other cities
    }
  }

  return downloaded;
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  const now = new Date();
  const rocYear = now.getFullYear() - 1911;
  const season = Math.ceil((now.getMonth() + 1) / 3);
  // Download previous season (current season data may not be published yet)
  const targetSeason = season === 1 ? 4 : season - 1;
  const targetYear = season === 1 ? rocYear - 1 : rocYear;

  console.log(`Downloading 實價登錄 data: ${targetYear}S${targetSeason}`);
  const files = await downloadPlvr({ year: targetYear, season: targetSeason });
  console.log(`\nDownloaded ${files.length} files to ${DATA_DIR}`);
}
