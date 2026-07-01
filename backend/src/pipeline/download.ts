/**
 * Download 實價登錄 open data CSVs from 內政部
 * Source: https://plvr.land.moi.gov.tw/DownloadOpenData
 *
 * The API serves ZIP files containing CSVs for each city/season.
 * URL pattern: GET with city code + data type + season params.
 */

import { mkdirSync, createWriteStream, existsSync, unlinkSync, renameSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable, Transform } from "node:stream";
import path from "node:path";
import { CITY_CODES } from "../types.js";

/** Sentinel error thrown when the server returns HTML instead of CSV data. */
class HtmlResponseError extends Error {
  constructor(city: string) {
    super(`HTML response for ${city} — season data not yet published`);
    this.name = "HtmlResponseError";
  }
}

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

      // Reject HTML responses: 內政部 returns 200 OK with an HTML error page
      // when data for the requested season is not yet published. Saving HTML as
      // CSV causes silent import failures downstream.
      const contentType = res.headers.get("content-type") ?? "";
      if (contentType.includes("text/html")) {
        console.warn(`[warn] ${cityCode}: server returned HTML (season data not available yet) — skipping`);
        continue;
      }

      // Write to a temp path first; rename on success to avoid partial files
      const tmpPath = `${outPath}.tmp`;
      const writable = createWriteStream(tmpPath);
      try {
        // Sniff the first non-empty chunk for HTML in case Content-Type is absent/wrong.
        // Buffer.from(chunk) is required: Node ≥ 18 stream Transforms pass Uint8Array,
        // not Buffer, so plain .toString("utf8") would produce "0,60,104,…" digit strings.
        // subarray is preferred over the deprecated Buffer.prototype.slice.
        let firstChunkSeen = false;
        const sniffingStream = new Transform({
          transform(chunk: Uint8Array, _enc, cb) {
            if (!firstChunkSeen && chunk.length > 0) {
              firstChunkSeen = true;
              const preview = Buffer.from(chunk).subarray(0, 64).toString("utf8").trimStart();
              if (preview.startsWith("<")) {
                cb(new HtmlResponseError(cityCode));
                return;
              }
            }
            cb(null, chunk);
          },
        });
        await pipeline(Readable.fromWeb(res.body as any), sniffingStream, writable);
        renameSync(tmpPath, outPath);
      } catch (writeErr) {
        // Clean up partial file. Only suppress ENOENT (file never created, e.g. the
        // error fired before any bytes were written); re-throw other filesystem errors.
        try { unlinkSync(tmpPath); } catch (unlinkErr) {
          if ((unlinkErr as NodeJS.ErrnoException).code !== "ENOENT") throw unlinkErr;
        }
        if (writeErr instanceof HtmlResponseError) {
          console.warn(`[warn] ${cityCode}: response body is HTML (season data not yet published) — skipping`);
          continue;
        }
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
  const currentSeason = Math.ceil((now.getMonth() + 1) / 3);

  // Build a list of up to 4 seasons to try, starting with the previous season
  // (current-season data is usually not yet published) and going back up to 3
  // quarters. The loop stops as soon as any season yields at least one CSV file.
  const candidates: Array<{ year: number; season: number }> = [];
  let s = currentSeason;
  let y = rocYear;
  for (let i = 0; i < 4; i++) {
    s -= 1;
    if (s === 0) { s = 4; y -= 1; }
    candidates.push({ year: y, season: s });
  }

  let files: string[] = [];
  for (const { year, season } of candidates) {
    console.log(`Downloading 實價登錄 data: ${year}S${season}`);
    try {
      const result = await downloadPlvr({ year, season });
      if (result.length > 0) {
        files = result;
        console.log(`\nDownloaded ${files.length} files to ${DATA_DIR}`);
        break;
      }
    } catch (err) {
      // Treat per-quarter network/HTTP errors as non-fatal and try next quarter
      console.warn(`[fallback] ${year}S${season} error: ${(err as Error).message}`);
    }
    console.log(`[fallback] ${year}S${season} returned no usable data — trying previous quarter`);
  }

  if (files.length === 0) {
    console.warn("[warn] No CSV files downloaded after trying 4 quarters — pipeline will import 0 records");
  }
}
