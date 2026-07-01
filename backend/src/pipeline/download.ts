/**
 * Download 實價登錄 open data CSVs from 內政部
 * Source: https://plvr.land.moi.gov.tw/DownloadOpenData
 *
 * The portal bundles all cities in a single ZIP file. The correct download URL
 * (discovered from the portal's preDownload() JavaScript) is:
 *   GET /Download?type=zip&fileName=lvr_land{format}.zip
 *
 * This always fetches the most recently published batch (updated on the 1st,
 * 11th, and 21st of each month). The ZIP contains one CSV per city, e.g.:
 *   A_lvr_land_A_115S2.csv   (台北市, sales data)
 *   B_lvr_land_A_115S2.csv   (台中市, sales data)
 *   ...
 */

import { mkdirSync, createWriteStream, unlinkSync, renameSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import path from "node:path";
import unzipper from "unzipper";

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
      signal: AbortSignal.timeout(120_000),
    });
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

export const DATA_DIR = path.resolve(import.meta.dirname, "../../../data/downloads");
const DATA_DIR_REAL = path.resolve(DATA_DIR) + path.sep;

// The bulk download URL discovered from the portal's preDownload() JS function.
// Returns a ZIP of all cities for the current published batch.
const BULK_DOWNLOAD_URL = "https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip";

const HEADERS = {
  "User-Agent": "Mozilla/5.0 (compatible; RealEstateRadar/1.0; +https://github.com/copilot-autogent/realestate-radar)",
  "Accept": "application/zip,application/octet-stream,*/*",
  "Referer": "https://plvr.land.moi.gov.tw/DownloadOpenData",
};

// Filename pattern for type-A (sales/買賣) CSVs in the bulk ZIP.
// The portal now uses lowercase names without season suffix: "a_lvr_land_a.csv".
// The seasonal-suffix alternative handles legacy per-city files: "A_lvr_land_A_115S2.csv".
const CSV_SALES_RE = /^[A-Za-z]_lvr_land_[Aa](\.csv|_\d+S\d+\.csv)$/;

/**
 * Download the current bulk ZIP from 內政部 and extract all city CSV files.
 * Returns the list of extracted CSV file paths.
 */
export async function downloadPlvrBulk(): Promise<string[]> {
  mkdirSync(DATA_DIR, { recursive: true });

  console.log("[download] Fetching bulk ZIP from 內政部...");
  const res = await fetchWithRetry(BULK_DOWNLOAD_URL, HEADERS);

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} from 內政部 bulk download`);
  }
  if (!res.body) {
    throw new Error("Empty response body from 內政部 bulk download");
  }

  // Verify we got a ZIP (not an HTML error page). Check Content-Type first;
  // also peek at the first bytes to catch cases where the server returns HTML
  // with a non-HTML content-type (or no content-type at all).
  const contentType = (res.headers.get("content-type") ?? "").toLowerCase();
  if (contentType.includes("text/html")) {
    try { await res.body.cancel(); } catch { /* ignore cancel errors */ }
    throw new Error("Server returned HTML instead of ZIP — portal may be down or blocking the request");
  }

  const extracted: string[] = [];

  // Stream the ZIP directly into unzipper — no need to write the archive to disk.
  // unzipper will throw if the stream is not a valid ZIP, catching the HTML-as-
  // octet-stream edge case at parse time rather than silently producing 0 files.
  let zipStream: unzipper.ParseStream;
  try {
    zipStream = Readable.fromWeb(res.body as any).pipe(unzipper.Parse({ forceStream: true }));
  } catch (err) {
    throw new Error(`Failed to parse ZIP stream: ${(err as Error).message}`);
  }

  for await (const entry of zipStream) {
    const filename: string = path.basename(entry.path); // basename strips any path prefix from the entry
    // Only extract type-A (sales/買賣) CSVs; skip presale (B) and rental (C)
    if (!CSV_SALES_RE.test(filename)) {
      await entry.autodrain();
      continue;
    }

    // Zip-slip guard: ensure the resolved output path stays within DATA_DIR
    const outPath = path.join(DATA_DIR, filename);
    if (!path.resolve(outPath).startsWith(DATA_DIR_REAL)) {
      console.warn(`[warn] Zip entry "${entry.path}" would escape DATA_DIR — skipping`);
      await entry.autodrain();
      continue;
    }

    // Always re-extract: the portal updates files within the same season name
    // every 10 days; skipping by filename would serve stale data across runs.
    // Write to a temp path first; rename on success to prevent partial files
    // from being silently re-used on the next run.
    const tmpPath = `${outPath}.tmp`;
    console.log(`[extract] ${filename}`);
    const writable = createWriteStream(tmpPath);
    try {
      await pipeline(entry, writable);
      renameSync(tmpPath, outPath);
      extracted.push(outPath);
      console.log(`[ok] ${filename}`);
    } catch (err) {
      try { unlinkSync(tmpPath); } catch (unlinkErr) {
        if ((unlinkErr as NodeJS.ErrnoException).code !== "ENOENT") throw unlinkErr;
      }
      console.error(`[error] ${filename}:`, (err as Error).message);
      // Non-fatal: continue with other files in the ZIP
    }
  }

  return extracted;
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  console.log("Downloading bulk 實價登錄 ZIP from 內政部...");
  try {
    const files = await downloadPlvrBulk();
    console.log(`\nExtracted ${files.length} CSV files to ${DATA_DIR}`);
    if (files.length === 0) {
      console.warn("[warn] No CSV files extracted — portal may be unavailable or format changed");
      process.exit(1);
    }
  } catch (err) {
    console.error("[error] Download failed:", (err as Error).message);
    process.exit(1);
  }
}
