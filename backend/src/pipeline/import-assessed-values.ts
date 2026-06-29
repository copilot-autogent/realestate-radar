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

import { fileURLToPath } from "node:url";
import { readFileSync, realpathSync } from "node:fs";
import path from "node:path";
import { parse } from "csv-parse/sync";
import iconv from "iconv-lite";
import { query, getPool, close } from "../db/connection.js";
import type { DistrictAssessedValue } from "../types.js";

/**
 * Normalize city/district names: convert 臺→台 and trim whitespace.
 * The 公告地價 dataset uses 臺 while PLVR and CITY_CODES use 台.
 */
function normalizeName(s: string): string {
  return s.trim().replace(/臺/g, "台");
}

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
  // UTF-8 BOM — strip BOM bytes before decoding
  if (buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
    return buffer.slice(3).toString("utf-8");
  }
  const utf8 = buffer.toString("utf-8");
  if (!utf8.includes("�")) return utf8;
  return iconv.decode(buffer, "big5");
}

function rocYearToAd(rocYear: string): number | null {
  const n = parseInt(rocYear, 10);
  // Sanity-check: valid ROC years are roughly 50–130 (AD 1961–2041)
  if (isNaN(n) || n < 50 || n > 130) return null;
  return n + 1911;
}

function safeFloat(val: string | undefined): number | null {
  if (!val) return null;
  const n = parseFloat(val.replace(/,/g, ""));
  return isNaN(n) ? null : n;
}

/**
 * Parse a 公告現值 CSV buffer into raw rows.
 * Tries known column-count variants; validates that the accepted variant's
 * column count matches the actual data column count to avoid positional mis-mapping.
 */
function parseAssessedValueCsv(content: string): AssessedValueRawRow[] {
  // First, probe how many columns the CSV actually has
  const firstDataLine = content.split("\n").find(l => l.trim().length > 0);
  const actualColCount = firstDataLine ? firstDataLine.split(",").length : 0;

  const columnVariants: { cols: string[]; colCount: number }[] = [
    // Variant 1: 6-column (no codes)
    { cols: ["縣市", "鄉鎮市區", "地號", "公告現值", "公告地價", "年度"], colCount: 6 },
    // Variant 2: 8-column (codes prepended)
    { cols: ["縣市代碼", "鄉鎮市區代碼", "縣市", "鄉鎮市區", "地號", "公告現值", "公告地價", "年度"], colCount: 8 },
  ];

  for (const { cols, colCount } of columnVariants) {
    // Only try if the CSV has at least as many columns as this variant expects
    if (actualColCount < colCount) continue;
    try {
      const rows = parse(content, {
        columns: cols,
        skip_empty_lines: true,
        relax_column_count: true,
        from_line: 2,  // skip header row
      }) as AssessedValueRawRow[];
      if (rows.length > 0 && rows[0].縣市 && rows[0].年度 && rocYearToAd(rows[0].年度) !== null) {
        return rows;
      }
    } catch {
      // try next variant
    }
  }

  // All known variants failed — fall back to auto-detect (may produce partial data)
  console.warn("[assessed] CSV column-variant detection failed; falling back to header auto-detect");
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

    const city = normalizeName(row.縣市 ?? "");
    const district = normalizeName(row.鄉鎮市區 ?? "");
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
  // Use a dedicated client so BEGIN/COMMIT/ROLLBACK run on the same connection
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    for (const d of districts) {
      await client.query(
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
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
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

  // Join transactions with the district_assessed_values for the closest available year.
  // Tie-break by preferring the more recent (larger) year when year_diff is equal.
  const result = await query(`
    WITH ranked AS (
      SELECT
        t.id AS tx_id,
        dav.year AS assessed_year,
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
        median_assessed_value_per_sqm
      FROM ranked
      ORDER BY tx_id, year_diff ASC, assessed_year DESC
    )
    UPDATE transactions t
    SET
      assessed_value_per_sqm = b.median_assessed_value_per_sqm,
      -- unit_price is stored in 元/坪; convert back to 元/sqm: unit_price / 3.30579
      -- Clamp ratio to NUMERIC(10,4) range; guard against zero/near-zero prices
      assessed_to_market_ratio = CASE
        WHEN (t.unit_price / 3.30579) > 0
        THEN LEAST(
          ROUND((b.median_assessed_value_per_sqm / (t.unit_price / 3.30579) * 100)::NUMERIC, 4),
          999999.9999
        )
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
    // Store with normalized names so lookupAssessedValue can find them reliably
    const key = `${normalizeName(row.city)}|${normalizeName(row.district)}|${row.year}`;
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
  const normCity = normalizeName(city);
  const normDistrict = normalizeName(district);
  // Try exact year first, then increasing distance.
  // On ties prefer the more recent (larger) year — consistent with backfill SQL
  // which uses ORDER BY year_diff ASC, assessed_year DESC.
  for (let delta = 0; delta <= 3; delta++) {
    const offsets = delta === 0 ? [0] : [delta, -delta]; // future before past = larger year first
    for (const offset of offsets) {
      const key = `${normCity}|${normDistrict}|${txYear + offset}`;
      const val = map.get(key);
      if (val !== undefined) return val;
    }
  }
  return null;
}

// CLI entry point: import a specific file or run backfill
const isMain = (() => {
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(process.argv[1]);
  } catch {
    return false;
  }
})();

if (isMain) {
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
