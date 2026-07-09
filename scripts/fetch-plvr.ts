#!/usr/bin/env node
/**
 * Standalone PLVR (實價登錄) data pipeline.
 *
 * Downloads the current bulk 實價登錄 ZIP from 內政部, parses all type-A
 * (residential sales 買賣) CSVs, geocodes each record to the centroid of its
 * city+district polygon, and writes a GeoJSON FeatureCollection to
 * frontend/public/data/transactions.json.
 *
 * No PostgreSQL required — uses district GeoJSON centroids for geocoding.
 *
 * Usage:
 *   tsx scripts/fetch-plvr.ts              # download + parse + write
 *   tsx scripts/fetch-plvr.ts --validate   # validate existing output (CI smoke test)
 *
 * Environment variables:
 *   PLVR_URL      Override the download URL (default: 内政部 bulk CSV ZIP)
 *   EXPORT_LIMIT  Max features to export (default: 10000)
 *   MIN_FEATURES  Minimum required features for smoke test (default: 100)
 *   DISTRICT_MIN  Minimum transactions guaranteed per (city, district) pair via
 *                 stratified sampling (default: 30).  Ensures secondary-city
 *                 districts (桃園中壢, 台中西屯, …) always have enough records
 *                 for reliable median / YoY computation even though they appear
 *                 less frequently in the national PLVR corpus than Taipei.
 */

import { readFileSync, writeFileSync, mkdirSync, readdirSync, renameSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Readable } from "node:stream";
import unzipper from "unzipper";
import { parse as csvParse } from "csv-parse/sync";
import iconv from "iconv-lite";
import {
  normalizeFullWidth,
  parseRocDate,
  parseRocBuildYear,
  detectCsvFormat,
  PLVR_COL_COUNT_LEGACY,
  PLVR_COL_COUNT_NEW,
} from "../frontend/src/lib/plvrUtils.js";
import {
  stratifiedSample,
  validateDistrictCoverage,
} from "../frontend/src/lib/stratifiedSample.js";

// ── Configuration ─────────────────────────────────────────────────────────────

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(__dirname, "..");
const OUT_PATH = join(REPO_ROOT, "frontend/public/data/transactions.json");
const DISTRICT_DATA_DIR = join(REPO_ROOT, "frontend/public/data");

const BULK_DOWNLOAD_URL =
  process.env.PLVR_URL ??
  "https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip";

const EXPORT_LIMIT = parseInt(process.env.EXPORT_LIMIT ?? "10000", 10);
const MIN_FEATURES = parseInt(process.env.MIN_FEATURES ?? "100", 10);
const DISTRICT_MIN_RAW = parseInt(process.env.DISTRICT_MIN ?? "30", 10);
// Clamp to ≥1: zero or negative would silently disable the per-district guarantee.
const DISTRICT_MIN = Number.isNaN(DISTRICT_MIN_RAW) || DISTRICT_MIN_RAW < 1 ? 30 : DISTRICT_MIN_RAW;

const REQUEST_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (compatible; RealEstateRadar/1.0; +https://github.com/copilot-autogent/realestate-radar)",
  Accept: "application/zip,application/octet-stream,*/*",
  Referer: "https://plvr.land.moi.gov.tw/DownloadOpenData",
};

// Matches type-A (sales) CSVs: a_lvr_land_a.csv or A_lvr_land_A_115S2.csv
const CSV_SALES_RE = /^[A-Za-z]_lvr_land_[Aa](\.csv|_\d+S\d+\.csv)$/;

// Column definitions for the two known PLVR CSV schemas
const CSV_COLUMNS_LEGACY = [
  "鄉鎮市區", "交易標的", "土地位置建物門牌", "土地移轉總面積平方公尺",
  "都市土地使用分區", "交易年月日", "交易筆棟數", "移轉層次", "總樓層數",
  "建物型態", "主要用途", "主要建材", "建築完成年月", "建物移轉總面積平方公尺",
  "建物現況格局房", "建物現況格局廳", "建物現況格局衛", "建物現況格局隔間",
  "有無管理組織", "總價元", "單價元平方公尺", "車位類別",
  "車位移轉總面積平方公尺", "車位總價元", "備註", "編號",
];
const CSV_COLUMNS_NEW = [
  "鄉鎮市區", "交易標的", "土地位置建物門牌", "土地移轉總面積平方公尺",
  "都市土地使用分區", "非都市土地使用分區", "非都市土地使用編定",
  "交易年月日", "交易筆棟數", "移轉層次", "總樓層數",
  "建物型態", "主要用途", "主要建材", "建築完成年月", "建物移轉總面積平方公尺",
  "建物現況格局房", "建物現況格局廳", "建物現況格局衛", "建物現況格局隔間",
  "有無管理組織", "總價元", "單價元平方公尺", "車位類別",
  "車位移轉總面積平方公尺", "車位總價元", "備註", "編號",
];

// Taiwan city code → 正體字 name (for mapping CSV city code to display name)
const CITY_CODES: Record<string, string> = {
  A: "台北市", B: "台中市", C: "基隆市", D: "台南市", E: "高雄市",
  F: "新北市", G: "宜蘭縣", H: "桃園市", I: "嘉義市", J: "新竹縣",
  K: "苗栗縣", L: "台中縣", M: "南投縣", N: "彰化縣", O: "新竹市",
  P: "雲林縣", Q: "嘉義縣", R: "台南縣", S: "高雄縣", T: "屏東縣",
  U: "花蓮縣", V: "台東縣", W: "金門縣", X: "澎湖縣", Z: "連江縣",
};

// ── Types ─────────────────────────────────────────────────────────────────────

interface GeoJsonPoint {
  type: "Point";
  coordinates: [number, number]; // [lon, lat]
}

interface TransactionFeature {
  type: "Feature";
  geometry: GeoJsonPoint;
  properties: {
    id: number;
    unitPrice: number;
    totalPrice: number;
    areaPing: number | null;
    buildingType: string | null;
    transactionType: string | null;
    date: string;
    address: string | null;
    city: string;
    district: string;
    floor: string | null;
    floorsTotal: number | null;
    rooms: number | null;
    buildYear: number | null;
    assessedValuePerSqm: null;
    assessedToMarketRatio: null;
  };
}

interface GeoJsonFeatureCollection {
  type: "FeatureCollection";
  features: TransactionFeature[];
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/** Decode CSV buffer: strip UTF-8 BOM or fall back to Big5 */
function detectAndDecode(buffer: Buffer): string {
  if (buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
    return buffer.toString("utf-8");
  }
  const utf8 = buffer.toString("utf-8");
  if (!utf8.includes("\ufffd")) return utf8;
  return iconv.decode(buffer, "big5");
}

/** Normalize city name: 臺→台 (PLVR CSV uses 臺; GeoJSON files use 台) */
function normalizeCityName(city: string): string {
  return city.replace(/臺/g, "台");
}

/** Compute the centroid of a GeoJSON Polygon's outer ring.
 * GeoJSON rings are closed (first vertex == last vertex), so we strip the
 * duplicate before averaging to avoid biasing the centroid. */
function polygonCentroid(coords: number[][]): { lon: number; lat: number } {
  // Deduplicate the closing vertex
  const open = coords.slice(0, -1);
  let lonSum = 0;
  let latSum = 0;
  for (const [lon, lat] of open) {
    lonSum += lon;
    latSum += lat;
  }
  return { lon: lonSum / open.length, lat: latSum / open.length };
}

/** Parse sqm area → 坪 (1 坪 = 3.30579 m², so divide; rounded to 1 decimal) */
function sqmToPing(sqm: number): number {
  return Math.round((sqm / 3.30579) * 10) / 10;
}

// ── District centroid lookup ──────────────────────────────────────────────────

type CentroidMap = Map<string, { lon: number; lat: number }>;

/**
 * Build a centroid lookup map from all *-districts.geojson files.
 * Key: "${normalizedCity}-${district}", Value: {lon, lat}
 */
function buildCentroidMap(): CentroidMap {
  const map: CentroidMap = new Map();

  let geojsonFiles: string[];
  try {
    geojsonFiles = readdirSync(DISTRICT_DATA_DIR).filter(
      f => f.endsWith("-districts.geojson"),
    );
  } catch (err) {
    console.warn(`[geocode] Cannot read district data dir: ${(err as Error).message}`);
    return map;
  }

  for (const filename of geojsonFiles) {
    const filepath = join(DISTRICT_DATA_DIR, filename);
    let data: { features: Array<{ properties: Record<string, string>; geometry: { type: string; coordinates: unknown } }> };
    try {
      data = JSON.parse(readFileSync(filepath, "utf-8"));
    } catch {
      console.warn(`[geocode] Failed to parse ${filename}`);
      continue;
    }

    for (const feature of data.features ?? []) {
      const { district, city } = feature.properties ?? {};
      if (!district || !city) continue;

      let outerRing: number[][] | undefined;
      if (feature.geometry?.type === "Polygon") {
        outerRing = (feature.geometry.coordinates as number[][][])[0];
      } else if (feature.geometry?.type === "MultiPolygon") {
        // Use the outer ring of the largest polygon (by vertex count) as the representative
        const rings = (feature.geometry.coordinates as number[][][][]).map(p => p[0]);
        outerRing = rings.reduce((best, r) => (r.length > best.length ? r : best), rings[0] ?? []);
      }
      if (!outerRing?.length) continue;

      const centroid = polygonCentroid(outerRing);
      const key = `${normalizeCityName(city)}-${district}`;
      if (!map.has(key)) map.set(key, centroid);
    }
  }

  console.log(`[geocode] Loaded ${map.size} district centroids from ${geojsonFiles.length} files`);
  return map;
}

// ── CSV parsing ───────────────────────────────────────────────────────────────

interface PlvrRawRecord {
  鄉鎮市區: string;
  交易標的: string;
  土地位置建物門牌: string;
  土地移轉總面積平方公尺: string;
  都市土地使用分區: string;
  交易年月日: string;
  交易筆棟數: string;
  移轉層次: string;
  總樓層數: string;
  建物型態: string;
  主要用途: string;
  主要建材: string;
  建築完成年月: string;
  建物移轉總面積平方公尺: string;
  建物現況格局房: string;
  建物現況格局廳: string;
  建物現況格局衛: string;
  建物現況格局隔間: string;
  有無管理組織: string;
  總價元: string;
  單價元平方公尺: string;
  車位類別: string;
  車位移轉總面積平方公尺: string;
  車位總價元: string;
  備註: string;
  編號: string;
}

/** Extract city name from CSV filename (e.g. "A_lvr_land_A.csv" → "台北市") */
function cityFromFilename(filename: string): string | null {
  const match = filename.match(/^([A-Za-z])_lvr_land_[Aa]/);
  if (!match) return null;
  return CITY_CODES[match[1].toUpperCase()] ?? null;
}

/**
 * Parse a single PLVR CSV buffer into transaction features.
 * Returns an array of (partially-geocoded) transaction records.
 */
function parseCsv(
  buffer: Buffer,
  filename: string,
  centroidMap: CentroidMap,
  idOffset: number,
): { features: TransactionFeature[]; skipped: number } {
  const city = cityFromFilename(filename);
  if (!city) {
    console.warn(`[parse] Cannot determine city from filename: ${filename}`);
    return { features: [], skipped: 0 };
  }

  const content = detectAndDecode(buffer);
  const lines = content.split("\n");

  if (lines.length < 3) {
    console.warn(`[parse] ${filename}: too few lines (${lines.length})`);
    return { features: [], skipped: 0 };
  }

  // Line 0: Chinese column headers; Line 1: English headers; data starts at Line 2
  const headerLine = (lines[0] ?? "").replace(/^\ufeff/, ""); // strip BOM char if present
  const format = detectCsvFormat(headerLine);
  const columns = format === "new28" ? CSV_COLUMNS_NEW : CSV_COLUMNS_LEGACY;
  const minExpectedCols = format === "new28" ? PLVR_COL_COUNT_NEW : PLVR_COL_COUNT_LEGACY;

  // Column-count validation: inspect the raw header row BEFORE csv-parse forces
  // our column names, so schema drift (upstream adding/removing columns at unexpected
  // positions) is caught accurately. csv-parse would silently mis-map fields otherwise.
  const rawHeaderCols = headerLine.split(",").length;
  if (rawHeaderCols < minExpectedCols) {
    console.warn(
      `[parse] ${filename}: raw header has ${rawHeaderCols} columns, expected ≥${minExpectedCols} ` +
      `(format: ${format}). Upstream schema may have shifted. Skipping file.`,
    );
    return { features: [], skipped: 0 };
  }
  if (rawHeaderCols > minExpectedCols + 5) {
    // Extra columns beyond expectation — log a warning but continue (relax_column_count handles them)
    console.warn(
      `[parse] ${filename}: unexpected extra columns (got ${rawHeaderCols}, expected ~${minExpectedCols}). ` +
      `Trailing columns will be ignored; consider updating CSV_COLUMNS_NEW.`,
    );
  }

  const dataContent = lines.slice(2).join("\n");

  let records: PlvrRawRecord[];
  try {
    records = csvParse(dataContent, {
      columns,
      skip_empty_lines: true,
      relax_column_count: true, // tolerate extra trailing columns
    }) as PlvrRawRecord[];
  } catch (err) {
    console.error(`[parse] CSV parse error in ${filename}: ${(err as Error).message}`);
    return { features: [], skipped: 0 };
  }

  const features: TransactionFeature[] = [];
  let skipped = 0;

  const currentYear = new Date().getFullYear();

  for (const rec of records) {
    // Only process residential sales with building component
    if (!rec.編號 || !rec.交易年月日 || !rec.總價元) { skipped++; continue; }
    // Filter to "房地" or "建物" transactions only (skip land-only)
    const txType = normalizeFullWidth(rec.交易標的 ?? "");
    if (!txType.includes("建物") && !txType.includes("房地")) { skipped++; continue; }
    // Filter to residential use only (住家用); skip commercial/office/industrial
    const mainUse = normalizeFullWidth(rec.主要用途 ?? "");
    if (mainUse && !mainUse.includes("住") && mainUse !== "") { skipped++; continue; }

    const totalPrice = parseInt(normalizeFullWidth(rec.總價元), 10);
    if (isNaN(totalPrice) || totalPrice <= 0) { skipped++; continue; }

    const dateStr = parseRocDate(normalizeFullWidth(rec.交易年月日));
    if (!dateStr) { skipped++; continue; }
    // Reject obviously-future dates (more than 1 year ahead — likely bad upstream data)
    const txYear = parseInt(dateStr.slice(0, 4), 10);
    if (txYear > currentYear + 1) { skipped++; continue; }

    const district = normalizeFullWidth(rec.鄉鎮市區 ?? "").trim();
    if (!district) { skipped++; continue; }

    // Geocode via district centroid
    const centroidKey = `${normalizeCityName(city)}-${district}`;
    const centroid = centroidMap.get(centroidKey);
    if (!centroid) { skipped++; continue; } // skip if district not in our GeoJSON files

    // Unit price: convert 元/sqm → 元/坪
    const unitPriceSqm = parseInt(normalizeFullWidth(rec.單價元平方公尺 ?? ""), 10);
    const unitPrice = isNaN(unitPriceSqm) || unitPriceSqm <= 0
      ? 0
      : Math.round(unitPriceSqm * 3.30579);
    if (unitPrice <= 0) { skipped++; continue; }

    const areaSqmStr = normalizeFullWidth(rec.建物移轉總面積平方公尺 ?? "");
    const areaSqm = parseFloat(areaSqmStr);
    const areaPing = isNaN(areaSqm) || areaSqm <= 0 ? null : sqmToPing(areaSqm);

    const rooms = parseInt(normalizeFullWidth(rec.建物現況格局房 ?? ""), 10);
    const floorsTotal = parseInt(normalizeFullWidth(rec.總樓層數 ?? ""), 10);

    const address = normalizeFullWidth(rec.土地位置建物門牌 ?? "").trim() || null;
    const buildingType = normalizeFullWidth(rec.建物型態 ?? "").trim() || null;
    const floor = normalizeFullWidth(rec.移轉層次 ?? "").trim() || null;

    const buildYear = parseRocBuildYear(normalizeFullWidth(rec.建築完成年月 ?? ""));

    features.push({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [centroid.lon, centroid.lat],
      },
      properties: {
        id: idOffset + features.length,
        unitPrice,
        totalPrice,
        areaPing,
        buildingType,
        transactionType: txType || null,
        date: dateStr,
        address,
        city: normalizeCityName(city),
        district,
        floor,
        floorsTotal: isNaN(floorsTotal) ? null : floorsTotal,
        rooms: isNaN(rooms) ? null : rooms,
        buildYear,
        assessedValuePerSqm: null,
        assessedToMarketRatio: null,
      },
    });
  }

  return { features, skipped };
}

// ── Download + extract ────────────────────────────────────────────────────────

interface ExtractedFile {
  filename: string;
  buffer: Buffer;
}

async function downloadAndExtract(): Promise<ExtractedFile[]> {
  console.log(`[download] Fetching ${BULK_DOWNLOAD_URL} ...`);

  const res = await fetch(BULK_DOWNLOAD_URL, {
    headers: REQUEST_HEADERS,
    signal: AbortSignal.timeout(180_000),
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} from 內政部 bulk download`);
  }
  if (!res.body) throw new Error("Empty response body");

  const contentType = (res.headers.get("content-type") ?? "").toLowerCase();
  if (contentType.includes("text/html")) {
    await res.body.cancel();
    throw new Error("Server returned HTML instead of ZIP — portal may be down");
  }

  const extracted: ExtractedFile[] = [];
  const zipStream = Readable.fromWeb(res.body as Parameters<typeof Readable.fromWeb>[0])
    .pipe(unzipper.Parse({ forceStream: true }));

  for await (const entry of zipStream) {
    const entryPath: string = (entry as { path: string }).path;
    const filename = entryPath.split(/[\\/]/).pop() ?? entryPath;

    if (!CSV_SALES_RE.test(filename)) {
      await (entry as { autodrain: () => Promise<void> }).autodrain();
      continue;
    }

    const chunks: Buffer[] = [];
    for await (const chunk of entry as AsyncIterable<Buffer>) {
      chunks.push(chunk);
    }
    const buf = Buffer.concat(chunks);
    extracted.push({ filename, buffer: buf });
    console.log(`[download] Extracted ${filename} (${(buf.byteLength / 1024).toFixed(0)} KB)`);
  }

  console.log(`[download] Extracted ${extracted.length} sales CSV file(s)`);
  return extracted;
}

// ── Smoke test / validation ───────────────────────────────────────────────────

const REQUIRED_FIELDS = [
  "id", "unitPrice", "totalPrice", "date", "city", "district",
] as const;

function validateOutput(geojson: unknown): void {
  const fc = geojson as GeoJsonFeatureCollection;
  if (!fc || fc.type !== "FeatureCollection" || !Array.isArray(fc.features)) {
    throw new Error("Output is not a valid GeoJSON FeatureCollection");
  }

  const count = fc.features.length;
  if (count < MIN_FEATURES) {
    throw new Error(
      `Smoke test FAILED: ${count} features < minimum ${MIN_FEATURES} required`,
    );
  }

  // Check required fields on all features
  let missingFields = 0;
  for (const feat of fc.features) {
    for (const field of REQUIRED_FIELDS) {
      if (feat.properties?.[field] === undefined || feat.properties?.[field] === null) {
        missingFields++;
      }
    }
  }
  if (missingFields > 0) {
    console.warn(`[validate] WARNING: ${missingFields} missing required field instances across features`);
  }

  // Check date range includes current year
  const currentYear = new Date().getFullYear();
  const dates = fc.features
    .map(f => f.properties?.date)
    .filter((d): d is string => typeof d === "string")
    .map(d => parseInt(d.slice(0, 4), 10))
    .filter(y => !isNaN(y));

  const maxYear = dates.length > 0 ? Math.max(...dates) : 0;
  if (maxYear < currentYear - 2) {
    console.warn(
      `[validate] WARNING: Most recent transaction year is ${maxYear}, ` +
      `which is more than 2 years before current year ${currentYear}. ` +
      `Data may be stale.`,
    );
  }

  console.log(
    `[validate] ✅ Smoke test passed: ${count} features, ` +
    `most recent year ${maxYear}, ${missingFields === 0 ? "all" : "some"} required fields present`,
  );
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const validateOnly = args.includes("--validate");

  if (validateOnly) {
    console.log("[validate] Running smoke test on existing output...");
    let existing: unknown;
    try {
      existing = JSON.parse(readFileSync(OUT_PATH, "utf-8"));
    } catch (err) {
      throw new Error(`Cannot read ${OUT_PATH}: ${(err as Error).message}`);
    }
    validateOutput(existing);
    // Per-district coverage check: threshold=10 (analytics minimum per spec).
    // Uses 10 not DISTRICT_MIN — DISTRICT_MIN is the sampling guarantee floor,
    // while 10 is the minimum for reliable analytics (sparkline, YoY, etc.).
    validateDistrictCoverage((existing as GeoJsonFeatureCollection).features);
    return;
  }

  // Download + extract CSVs
  const csvFiles = await downloadAndExtract();
  if (csvFiles.length === 0) {
    throw new Error("No type-A sales CSV files found in the downloaded ZIP");
  }

  // Build district centroid lookup
  const centroidMap = buildCentroidMap();
  if (centroidMap.size === 0) {
    throw new Error("No district centroids loaded — check frontend/public/data/*-districts.geojson");
  }

  // Parse all CSVs into features
  const allFeatures: TransactionFeature[] = [];
  let totalSkipped = 0;

  for (const { filename, buffer } of csvFiles) {
    const { features, skipped } = parseCsv(buffer, filename, centroidMap, allFeatures.length + 1);
    console.log(`[parse] ${filename}: ${features.length} features, ${skipped} skipped`);
    allFeatures.push(...features);
    totalSkipped += skipped;
  }

  // Stratified sampling: guarantee DISTRICT_MIN records per (city, district),
  // then fill remaining budget with globally most-recent.
  const exportFeatures = stratifiedSample(allFeatures, {
    districtMin: DISTRICT_MIN,
    exportLimit: EXPORT_LIMIT,
  });

  console.log(
    `[pipeline] ${exportFeatures.length} features → output (${totalSkipped} skipped across all files, ` +
    `DISTRICT_MIN=${DISTRICT_MIN})`,
  );

  const geojson: GeoJsonFeatureCollection = {
    type: "FeatureCollection",
    features: exportFeatures,
  };

  // Validate before writing
  validateOutput(geojson);
  // Per-district coverage check: threshold=10 (analytics minimum per spec).
  // DISTRICT_MIN is the sampling guarantee floor; 10 is the analytics minimum
  // (sparkline, YoY, similar-district). Intentionally non-fatal — warns only.
  validateDistrictCoverage(exportFeatures);

  // Atomic write: write to .tmp first, then rename to avoid partial-write corruption
  const tmpPath = OUT_PATH + ".tmp";
  mkdirSync(dirname(OUT_PATH), { recursive: true });
  writeFileSync(tmpPath, JSON.stringify(geojson));
  renameSync(tmpPath, OUT_PATH);
  console.log(`[output] Written to ${OUT_PATH}`);
}

// ── CLI entry ─────────────────────────────────────────────────────────────────

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err => {
    console.error("[fatal]", err.message);
    process.exit(1);
  });
}

export { parseCsv, buildCentroidMap, validateOutput };
