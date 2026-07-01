/**
 * Import 實價登錄 CSV data into PostGIS
 *
 * Reads CSVs downloaded by download.ts, parses them,
 * and upserts into the transactions table.
 *
 * CSV encoding: UTF-8 with BOM (newer files) or Big5 (older files)
 *
 * Environment variables:
 *   DATA_SOURCE=live  — enable live pipeline mode (required for production).
 *                       When absent, warns and exits early (sample data fallback
 *                       remains in frontend/public/data/sample-transactions.json).
 */

import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { parse } from "csv-parse/sync";
import iconv from "iconv-lite";
import { query, close } from "../db/connection.js";
import { parseRocDate, sqmToPing, CITY_CODES, type PlvrRawRecord } from "../types.js";
import { loadDistrictAssessedValueMap, lookupAssessedValue } from "./import-assessed-values.js";

const DATA_DIR = path.resolve(import.meta.dirname, "../../../data/downloads");

function detectAndDecode(buffer: Buffer): string {
  // Check for UTF-8 BOM
  if (buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
    return buffer.toString("utf-8");
  }
  // Try UTF-8 first
  const utf8 = buffer.toString("utf-8");
  if (!utf8.includes("�")) return utf8;
  // Fall back to Big5
  return iconv.decode(buffer, "big5");
}

function safeInt(val: string | undefined): number | null {
  if (!val) return null;
  const n = parseInt(val, 10);
  return isNaN(n) ? null : n;
}

function safeBigint(val: string | undefined): number | null {
  if (!val) return null;
  const n = Number(val);
  return isNaN(n) ? null : n;
}

function safeFloat(val: string | undefined): number | null {
  if (!val) return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : n;
}

/** Extract city code from a sales CSV filename.
 * Accepts both the current lowercase format ("a_lvr_land_a.csv") and the
 * older uppercase seasonal format ("A_lvr_land_A_115S2.csv").
 * Returns uppercase letter for CITY_CODES lookup, or null if not matched.
 */
function extractCityCode(filename: string): string | null {
  // Type-A (sales) files only: _lvr_land_a or _lvr_land_A
  const match = filename.match(/^([A-Za-z])_lvr_land_[Aa]/);
  return match ? match[1].toUpperCase() : null;
}

/**
 * Query the DB for the most recent transaction_date already imported.
 * Returns null only when the table is empty (no data yet) or doesn't exist yet
 * (first-run before schema is applied). Re-throws on genuine DB errors.
 */
async function getMaxTransactionDate(): Promise<Date | null> {
  try {
    const result = await query("SELECT MAX(transaction_date) AS max_date FROM transactions");
    const val = result.rows[0]?.max_date;
    return val ? new Date(val) : null;
  } catch (err) {
    const pgCode = (err as { code?: string }).code;
    if (pgCode === "42P01") {
      // Table doesn't exist yet — schema hasn't been applied; do a full import
      return null;
    }
    throw err;
  }
}

async function importFile(filepath: string, cutoffDate: Date | null = null): Promise<number> {
  const filename = path.basename(filepath);
  const cityCode = extractCityCode(filename);
  if (!cityCode) {
    console.warn(`[skip] Can't extract city code from ${filename}`);
    return 0;
  }
  const city = CITY_CODES[cityCode] ?? cityCode;

  console.log(`[import] ${filename} → ${city}`);

  // Log import start
  const logResult = await query(
    `INSERT INTO import_log (source, filename, status) VALUES ('plvr', $1, 'running') RETURNING id`,
    [filename]
  );
  const logId = logResult.rows[0].id;

  // Load district assessed values for ratio computation.
  // Graceful degradation: if the migration hasn't been applied yet (pg error 42P01 =
  // undefined_table), proceed without assessed data. Re-throw other errors.
  let assessedMap = new Map<string, number>();
  try {
    assessedMap = await loadDistrictAssessedValueMap();
  } catch (err) {
    const pgCode = (err as { code?: string }).code;
    if (pgCode === "42P01") {
      console.warn("[import] district_assessed_values table not found — run migration 001_add_assessed_values.sql");
    } else {
      throw err;
    }
  }

  const buffer = readFileSync(filepath);
  const content = detectAndDecode(buffer);

  // Skip header rows: first 2 rows are headers (Chinese column names + English)
  const lines = content.split("\n");
  const dataLines = lines.slice(2).join("\n");

  let records: PlvrRawRecord[];
  try {
    records = parse(dataLines, {
      columns: [
        "鄉鎮市區", "交易標的", "土地位置建物門牌", "土地移轉總面積平方公尺",
        // New bulk-ZIP format (2025+) inserts two extra columns here vs older seasonal format:
        "都市土地使用分區", "非都市土地使用分區", "非都市土地使用編定",
        "交易年月日", "交易筆棟數", "移轉層次", "總樓層數",
        "建物型態", "主要用途", "主要建材", "建築完成年月", "建物移轉總面積平方公尺",
        "建物現況格局房", "建物現況格局廳", "建物現況格局衛", "建物現況格局隔間",
        "有無管理組織", "總價元", "單價元平方公尺", "車位類別",
        "車位移轉總面積平方公尺", "車位總價元", "備註", "編號",
        // Extra trailing columns (e.g. additional land/building sub-records) are ignored
        // by relax_column_count below.
      ],
      skip_empty_lines: true,
      relax_column_count: true,
    }) as PlvrRawRecord[];
  } catch (err) {
    console.error(`[error] CSV parse failed for ${filename}:`, (err as Error).message);
    await query(`UPDATE import_log SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2`,
      [(err as Error).message, logId]);
    return 0;
  }

  let imported = 0;
  let skipped = 0;

  for (const rec of records) {
    if (!rec.編號 || !rec.交易年月日 || !rec.總價元) continue;

    const txDate = parseRocDate(rec.交易年月日);
    if (!txDate) continue;

    // Incremental update: skip records strictly before the existing max date.
    // Using strict `<` (not `<=`) so records on the boundary date are always
    // re-attempted — ON CONFLICT DO UPDATE makes the upsert idempotent.
    if (cutoffDate !== null && txDate < cutoffDate) {
      skipped++;
      continue;
    }

    const areaSqm = safeFloat(rec.建物移轉總面積平方公尺);
    const pricePerSqm = safeBigint(rec.單價元平方公尺);
    const unitPricePing = pricePerSqm ? sqmToPing(pricePerSqm) : null;

    // Look up district-level assessed value for this transaction's city/district/year
    const txYear = txDate.getFullYear();
    const assessedValuePerSqm = lookupAssessedValue(assessedMap, city, rec.鄉鎮市區, txYear);
    // Ratio: assessedValue / marketPrice(元/sqm) × 100; clamp to NUMERIC(10,4) max
    const rawRatio =
      assessedValuePerSqm !== null && pricePerSqm !== null && pricePerSqm > 0
        ? Math.round((assessedValuePerSqm / pricePerSqm) * 100 * 10000) / 10000
        : null;
    const assessedToMarketRatio = rawRatio !== null ? Math.min(rawRatio, 999999.9999) : null;
    // Only store assessed fields atomically: skip both if assessed value is unknown
    const storeAssessed = assessedValuePerSqm !== null && assessedToMarketRatio !== null;

    try {
      await query(
        `INSERT INTO transactions (
          city, district, address, transaction_date, transaction_type,
          total_price, unit_price, area_sqm,
          building_type, floors_total, floor, build_year,
          rooms, halls, bathrooms,
          has_parking, parking_type, parking_price, parking_area,
          land_use, note, serial_number, source_file,
          assessed_value_per_sqm, assessed_to_market_ratio
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8,
          $9, $10, $11, $12,
          $13, $14, $15,
          $16, $17, $18, $19,
          $20, $21, $22, $23,
          $24, $25
        ) ON CONFLICT (serial_number) DO UPDATE SET
          assessed_value_per_sqm = CASE
            WHEN EXCLUDED.assessed_value_per_sqm IS NOT NULL AND EXCLUDED.assessed_to_market_ratio IS NOT NULL
            THEN EXCLUDED.assessed_value_per_sqm
            ELSE transactions.assessed_value_per_sqm
          END,
          assessed_to_market_ratio = CASE
            WHEN EXCLUDED.assessed_value_per_sqm IS NOT NULL AND EXCLUDED.assessed_to_market_ratio IS NOT NULL
            THEN EXCLUDED.assessed_to_market_ratio
            ELSE transactions.assessed_to_market_ratio
          END`,
        [
          city,
          rec.鄉鎮市區,
          rec.土地位置建物門牌 || null,
          txDate.toISOString().split("T")[0],
          rec.交易標的,
          safeBigint(rec.總價元),
          unitPricePing,
          areaSqm,
          rec.建物型態 || null,
          safeInt(rec.總樓層數),
          rec.移轉層次 || null,
          safeInt(rec.建築完成年月),
          safeInt(rec.建物現況格局房),
          safeInt(rec.建物現況格局廳),
          safeInt(rec.建物現況格局衛),
          (rec.車位類別 ?? "").length > 0,
          rec.車位類別 || null,
          safeBigint(rec.車位總價元),
          safeFloat(rec.車位移轉總面積平方公尺),
          rec.都市土地使用分區 || null,
          rec.備註 || null,
          rec.編號,
          filename,
          storeAssessed ? assessedValuePerSqm : null,
          storeAssessed ? assessedToMarketRatio : null,
        ]
      );
      imported++;
    } catch (err) {
      // Skip individual record errors (dedup conflicts, etc.)
    }
  }

  await query(
    `UPDATE import_log SET status = 'success', record_count = $1, completed_at = NOW() WHERE id = $2`,
    [imported, logId]
  );

  console.log(`[ok] ${filename}: ${imported}/${records.length} records imported (${skipped} skipped as already-seen)`);
  return imported;
}

async function importAll(): Promise<void> {
  const files = readdirSync(DATA_DIR).filter(f => f.endsWith(".csv")).sort();
  if (files.length === 0) {
    console.log("No CSV files found. Run pipeline:download first.");
    return;
  }

  console.log(`Found ${files.length} CSV files to import`);

  // Incremental update: fetch the current high-water mark once before processing any file.
  // Normalize to UTC midnight to match parseRocDate's local-midnight semantics at the boundary.
  const rawCutoff = await getMaxTransactionDate();
  const cutoffDate = rawCutoff
    ? new Date(Date.UTC(rawCutoff.getFullYear(), rawCutoff.getMonth(), rawCutoff.getDate()))
    : null;
  if (cutoffDate) {
    console.log(`[incremental] Skipping records strictly before ${cutoffDate.toISOString().split("T")[0]} (boundary date re-imported for completeness)`);
  } else {
    console.log("[incremental] No existing records — performing full import");
  }

  let total = 0;

  for (const file of files) {
    const count = await importFile(path.join(DATA_DIR, file), cutoffDate);
    total += count;
  }

  // Refresh materialized view
  console.log("[refresh] district_price_stats materialized view...");
  await query("REFRESH MATERIALIZED VIEW CONCURRENTLY district_price_stats");

  console.log(`\nImport complete: ${total} new records`);
  await close();
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  // Guard: live pipeline requires explicit DATA_SOURCE=live to prevent
  // accidentally running against a dev DB without intending to.
  if (process.env.DATA_SOURCE !== "live") {
    // Exit 1 (not 0) so misconfigured CI runs fail visibly rather than
    // silently producing stale data.
    console.error(
      "[error] DATA_SOURCE is not 'live' — live 內政部 ingestion requires DATA_SOURCE=live.\n" +
      "        For local dev without PostGIS, use the sample-data fallback in\n" +
      "        frontend/public/data/sample-transactions.json directly."
    );
    process.exit(1);
  }
  await importAll();
}

export { importFile, importAll };
