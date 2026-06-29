/**
 * Import 公告地價 (Announced Land Price / 公告現值) data from 內政部地政司
 *
 * Implements Option C from issue #76: district-level aggregation.
 * Downloads/reads the CSV from 內政部地政司 公告地價 dataset and aggregates
 * 公告現值 to district-level median per year, storing in district_assessed_values.
 *
 * Dataset source: https://land.moi.gov.tw/chhtml/content/134
 *
 * CSV format (公告現值 / 公告地價 dataset):
 *   縣市代碼, 鄉鎮市區代碼, 縣市, 鄉鎮市區, 地號, 公告現值, 公告地價, 年度
 *
 * After ingesting this table, call backfillTransactionAssessedValues() to
 * update transactions with district-level assessed values.
 */

import { readFileSync, existsSync } from "node:fs";
import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { parse } from "csv-parse/sync";
import iconv from "iconv-lite";
import { query, close } from "../db/connection.js";
import type { DistrictAssessedValue } from "../types.js";

const DATA_DIR = path.resolve(import.meta.dirname, "../../../data/downloads");
const ASSESSED_DIR = path.resolve(import.meta.dirname, "../../../data/assessed");

/** Raw CSV row from 內政部地政司 公告現值 dataset */
interface AssessedValueRawRow {
  縣市: string;
  鄉鎮市區: string;
  地號?: string;
  公告現值: string;  // 元/平方公尺
  公告地價?: string;
  年度: string;      // ROC year e.g. "113"
}

function detectAndDecode(buffer: Buffer): string {
  if (buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
    return buffer.toString("utf-8");
  }
  const utf8 = buffer.toString("utf-8");
  if (!utf8.includes("�")) return utf8;
  return iconv.decode(buffer, "big5");
}

function rocYearToAd(rocYear: string): number | null {
  const n = parseInt(rocYear, 10);
  return isNaN(n) ? null : n + 1911;
}

function safeFloat(val: string | undefined): number | null {
  if (!val) return null;
  const n = parseFloat(val.replace(/,/g, ""));
  return isNaN(n) ? null : n;
}

/**
 * Parse a 公告現值 CSV buffer into raw rows.
 * The dataset has no fixed number of header rows — tries with and without skipping first row.
 */
function parseAssessedValueCsv(content: string): AssessedValueRawRow[] {
  const columnVariants = [
    // Variant 1: standard column names
    ["縣市", "鄉鎮市區", "地號", "公告現值", "公告地價", "年度"],
    // Variant 2: codes prepended
    ["縣市代碼", "鄉鎮市區代碼", "縣市", "鄉鎮市區", "地號", "公告現值", "公告地價", "年度"],
  ];

  for (const cols of columnVariants) {
    try {
      const rows = parse(content, {
        columns: cols,
        skip_empty_lines: true,
        relax_column_count: true,
        from_line: 2,  // skip header row
      }) as AssessedValueRawRow[];
      if (rows.length > 0 && rows[0].縣市 && rows[0].年度) return rows;
    } catch {
      // try next variant
    }
  }

  // Fallback: auto-detect columns from first row
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
  }) as AssessedValueRawRow[];
}

/**
 * Aggregate raw rows into district-level medians per year.
 */
function aggregateToDistrict(rows: AssessedValueRawRow[]): DistrictAssessedValue[] {
  // Group by (city, district, year)
  const groups = new Map<string, number[]>();
  const yearMap = new Map<string, number>();

  for (const row of rows) {
    const value = safeFloat(row.公告現值);
    if (value === null || value <= 0) continue;

    const adYear = rocYearToAd(row.年度);
    if (!adYear) continue;

    const city = (row.縣市 ?? "").trim();
    const district = (row.鄉鎮市區 ?? "").trim();
    if (!city || !district) continue;

    const key = `${city}|${district}|${adYear}`;
    if (!groups.has(key)) {
      groups.set(key, []);
      yearMap.set(key, adYear);
    }
    groups.get(key)!.push(value);
  }

  const result: DistrictAssessedValue[] = [];

  for (const [key, values] of groups) {
    if (values.length === 0) continue;
    const [city, district] = key.split("|");
    const year = yearMap.get(key)!;

    // Compute median
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid];

    result.push({
      city,
      district,
      year,
      medianAssessedValuePerSqm: Math.round(median * 100) / 100,
      parcelCount: values.length,
    });
  }

  return result;
}

/**
 * Load and ingest a 公告現值 CSV file into district_assessed_values table.
 * @param filepath Path to the CSV file (local)
 * @returns Number of district-year records upserted
 */
export async function importAssessedValueFile(filepath: string): Promise<number> {
  const filename = path.basename(filepath);
  console.log(`[assessed] Importing ${filename}`);

  const buffer = readFileSync(filepath);
  const content = detectAndDecode(buffer);
  const rows = parseAssessedValueCsv(content);

  if (rows.length === 0) {
    console.warn(`[assessed] No rows parsed from ${filename}`);
    return 0;
  }

  console.log(`[assessed] Parsed ${rows.length} raw rows from ${filename}`);

  const districts = aggregateToDistrict(rows);
  console.log(`[assessed] Aggregated to ${districts.length} district-year records`);

  let upserted = 0;
  for (const d of districts) {
    await query(
      `INSERT INTO district_assessed_values
         (city, district, year, median_assessed_value_per_sqm, parcel_count, source_file)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (city, district, year) DO UPDATE SET
         median_assessed_value_per_sqm = EXCLUDED.median_assessed_value_per_sqm,
         parcel_count = EXCLUDED.parcel_count,
         source_file = EXCLUDED.source_file,
         imported_at = NOW()`,
      [d.city, d.district, d.year, d.medianAssessedValuePerSqm, d.parcelCount, filename]
    );
    upserted++;
  }

  console.log(`[assessed] Upserted ${upserted} district-year records`);
  return upserted;
}

/**
 * Back-fill transactions with district-level assessed values.
 *
 * For each transaction, finds the closest available year in district_assessed_values
 * (same city+district) and sets assessed_value_per_sqm and assessed_to_market_ratio.
 *
 * Uses the unit price in 元/平方公尺 (stored as unit_price * 1/3.30579 to convert back from 坪).
 * Ratio = assessed_value_per_sqm / unit_price_sqm × 100
 */
export async function backfillTransactionAssessedValues(): Promise<number> {
  console.log("[assessed] Back-filling transaction assessed values...");

  // Join transactions with the district_assessed_values for the closest available year
  const result = await query(`
    WITH ranked AS (
      SELECT
        t.id AS tx_id,
        dav.median_assessed_value_per_sqm,
        ABS(dav.year - EXTRACT(YEAR FROM t.transaction_date)::INTEGER) AS year_diff
      FROM transactions t
      JOIN district_assessed_values dav
        ON dav.city = t.city AND dav.district = t.district
      WHERE t.unit_price IS NOT NULL AND t.unit_price > 0
    ),
    best AS (
      SELECT DISTINCT ON (tx_id)
        tx_id,
        median_assessed_value_per_sqm,
        year_diff
      FROM ranked
      ORDER BY tx_id, year_diff ASC
    )
    UPDATE transactions t
    SET
      assessed_value_per_sqm = b.median_assessed_value_per_sqm,
      -- unit_price is stored in 元/坪; convert back to 元/sqm: unit_price / 3.30579
      assessed_to_market_ratio = CASE
        WHEN (t.unit_price / 3.30579) > 0
        THEN ROUND((b.median_assessed_value_per_sqm / (t.unit_price / 3.30579) * 100)::NUMERIC, 4)
        ELSE NULL
      END
    FROM best b
    WHERE t.id = b.tx_id
    RETURNING t.id
  `);

  const count = result.rowCount ?? 0;
  console.log(`[assessed] Updated ${count} transactions with assessed values`);
  return count;
}

/**
 * Get a lookup map of (city|district|year) → median assessed value per sqm.
 * Used during live import to set assessed_value_per_sqm per record.
 */
export async function loadDistrictAssessedValueMap(): Promise<Map<string, number>> {
  const result = await query(
    `SELECT city, district, year, median_assessed_value_per_sqm
     FROM district_assessed_values`
  );

  const map = new Map<string, number>();
  for (const row of result.rows) {
    const key = `${row.city}|${row.district}|${row.year}`;
    map.set(key, parseFloat(row.median_assessed_value_per_sqm));
  }
  return map;
}

/**
 * Given a city, district and transaction year, look up the best matching
 * assessed value per sqm from the provided map (closest year).
 */
export function lookupAssessedValue(
  map: Map<string, number>,
  city: string,
  district: string,
  txYear: number,
): number | null {
  // Try exact year first, then ±1, ±2, ±3
  for (let delta = 0; delta <= 3; delta++) {
    for (const offset of delta === 0 ? [0] : [delta, -delta]) {
      const key = `${city}|${district}|${txYear + offset}`;
      const val = map.get(key);
      if (val !== undefined) return val;
    }
  }
  return null;
}

// CLI entry point: import a specific file or run backfill
if (import.meta.url === `file://${process.argv[1]}`) {
  const [, , cmd, filepath] = process.argv;

  if (cmd === "import" && filepath) {
    await importAssessedValueFile(filepath);
  } else if (cmd === "backfill") {
    await backfillTransactionAssessedValues();
  } else {
    console.log("Usage:");
    console.log("  tsx import-assessed-values.ts import <path/to/csv>");
    console.log("  tsx import-assessed-values.ts backfill");
  }

  await close();
}
