/**
 * Import 實價登錄 CSV data into PostGIS
 *
 * Reads CSVs downloaded by download.ts, parses them,
 * and upserts into the transactions table.
 *
 * CSV encoding: UTF-8 with BOM (newer files) or Big5 (older files)
 */

import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { parse } from "csv-parse/sync";
import iconv from "iconv-lite";
import { query, close } from "../db/connection.js";
import { parseRocDate, sqmToPing, CITY_CODES, type PlvrRawRecord } from "../types.js";

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

/** Extract city code from filename like "A_lvr_land_A_113S4.csv" */
function extractCityCode(filename: string): string | null {
  const match = filename.match(/^([A-Z])_lvr_land/);
  return match ? match[1] : null;
}

async function importFile(filepath: string): Promise<number> {
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
        "都市土地使用分區", "交易年月日", "交易筆棟數", "移轉層次", "總樓層數",
        "建物型態", "主要用途", "主要建材", "建築完成年月", "建物移轉總面積平方公尺",
        "建物現況格局房", "建物現況格局廳", "建物現況格局衛", "建物現況格局隔間",
        "有無管理組織", "總價元", "單價元平方公尺", "車位類別",
        "車位移轉總面積平方公尺", "車位總價元", "備註", "編號",
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

  for (const rec of records) {
    if (!rec.編號 || !rec.交易年月日 || !rec.總價元) continue;

    const txDate = parseRocDate(rec.交易年月日);
    if (!txDate) continue;

    const areaSqm = safeFloat(rec.建物移轉總面積平方公尺);
    const pricePerSqm = safeBigint(rec.單價元平方公尺);
    const unitPricePing = pricePerSqm ? sqmToPing(pricePerSqm) : null;

    try {
      await query(
        `INSERT INTO transactions (
          city, district, address, transaction_date, transaction_type,
          total_price, unit_price, area_sqm,
          building_type, floors_total, floor, build_year,
          rooms, halls, bathrooms,
          has_parking, parking_type, parking_price, parking_area,
          land_use, note, serial_number, source_file
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8,
          $9, $10, $11, $12,
          $13, $14, $15,
          $16, $17, $18, $19,
          $20, $21, $22, $23
        ) ON CONFLICT (serial_number) DO NOTHING`,
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

  console.log(`[ok] ${filename}: ${imported}/${records.length} records imported`);
  return imported;
}

async function importAll(): Promise<void> {
  const files = readdirSync(DATA_DIR).filter(f => f.endsWith(".csv")).sort();
  if (files.length === 0) {
    console.log("No CSV files found. Run pipeline:download first.");
    return;
  }

  console.log(`Found ${files.length} CSV files to import`);
  let total = 0;

  for (const file of files) {
    const count = await importFile(path.join(DATA_DIR, file));
    total += count;
  }

  // Refresh materialized view
  console.log("[refresh] district_price_stats materialized view...");
  await query("REFRESH MATERIALIZED VIEW CONCURRENTLY district_price_stats");

  console.log(`\nImport complete: ${total} total records`);
  await close();
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  await importAll();
}

export { importFile, importAll };
