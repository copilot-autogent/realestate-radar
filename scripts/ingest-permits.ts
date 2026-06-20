#!/usr/bin/env tsx
/**
 * Ingest permit data from 內政部不動產資訊平台 E3030
 * Source: pip.moi.gov.tw/Publicize/Info/E3030
 * Operator: 內政部國土管理署
 * License: 政府資料開放授權條款-第1版 (CC-BY equivalent)
 *
 * Fields fetched:
 *   建造執照總宅數 (building_permits)  → type=2, k=4, n=1
 *   使用執照總宅數 (occupancy_permits) → type=2, k=4, n=3
 *   建物開工總宅數 (starts)            → type=2, k=3, n=2
 *
 * Granularity: 縣市 (city level). Quarterly (latest: 114Q4 = Q4 2025).
 *
 * District allocation: pip.moi.gov.tw provides city-level totals only.
 * District-level data uses the proportional model from E5010 (future work).
 *
 * Usage:
 *   cd backend && tsx ../scripts/ingest-permits.ts [--dry-run] [--output path]
 *   --dry-run   Fetch and parse but do not write to DB
 *   --output    Write parsed JSON to file (for frontend static data)
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { query, close } from "../backend/src/db/connection.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BASE_URL = "https://pip.moi.gov.tw/Publicize/Info/E3030";

/** ROC year → AD year */
const ROC_BASE = 1911;

/** Permit type codes for the E3030 form download endpoint */
const PERMIT_TYPES = {
  building_permits: { t: "2", k: "4", n: "1" },   // 建造執照總宅數
  occupancy_permits: { t: "2", k: "4", n: "3" },  // 使用執照總宅數
  starts: { t: "2", k: "3", n: "2" },             // 建物開工總宅數
} as const;

type PermitKind = keyof typeof PERMIT_TYPES;

/** 6 major cities + their common alt-spellings for normalisation */
const CITY_ALIASES: Record<string, string> = {
  "臺北市": "台北市",
  "台北市": "台北市",
  "新北市": "新北市",
  "桃園市": "桃園市",
  "臺中市": "台中市",
  "台中市": "台中市",
  "臺南市": "台南市",
  "台南市": "台南市",
  "高雄市": "高雄市",
};

const SIX_CITIES = ["台北市", "新北市", "桃園市", "台中市", "台南市", "高雄市"] as const;
type City = typeof SIX_CITIES[number];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PermitRecord {
  city: string;
  district: string | null;  // null = city-level aggregate
  quarter: string;          // e.g. "2024Q1"
  building_permits: number;
  occupancy_permits: number;
  starts: number;
}

export interface PermitsJson {
  _comment: string;
  generated: string;
  source: string;
  note: string;
  quarters: string[];
  first_projected_quarter: string;
  cities: Record<City, {
    building_permits: number[];
    occupancy_permits: number[];
    starts: number[];
  }>;
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/** Fetch with retry + Taiwan-government UA. */
async function fetchWithRetry(url: string, opts: RequestInit = {}, retries = 3): Promise<Response> {
  const headers = {
    "User-Agent": "RealEstateRadar/0.1 (github.com/copilot-autogent/realestate-radar; data research)",
    "Accept": "text/csv,text/html,application/xhtml+xml,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://pip.moi.gov.tw/Publicize/Info/E3030",
    ...(opts.headers ?? {}),
  };

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { ...opts, headers, signal: AbortSignal.timeout(30_000) });
      if (res.ok) return res;
      if (res.status === 429 || res.status >= 500) {
        const wait = 2000 * (attempt + 1);
        console.warn(`[retry] HTTP ${res.status} — waiting ${wait}ms (attempt ${attempt + 1}/${retries})`);
        await sleep(wait);
        continue;
      }
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    } catch (err) {
      if (attempt === retries) throw err;
      const wait = 2000 * (attempt + 1);
      console.warn(`[retry] ${(err as Error).message} — waiting ${wait}ms (attempt ${attempt + 1}/${retries})`);
      await sleep(wait);
    }
  }
  throw new Error("Unreachable");
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// E3030 session & CSRF token acquisition
// ---------------------------------------------------------------------------

interface E3030Session {
  cookies: string;
  csrfToken: string;
}

/**
 * Acquire a session cookie and CSRF token from the E3030 page.
 * The site uses a standard ASP.NET __RequestVerificationToken pattern.
 */
async function acquireE3030Session(): Promise<E3030Session | null> {
  try {
    const res = await fetchWithRetry(BASE_URL);
    const cookies = res.headers.get("set-cookie") ?? "";
    const html = await res.text();

    // Extract __RequestVerificationToken from hidden input or meta tag
    const tokenMatch =
      html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/) ??
      html.match(/<meta[^>]+name="__RequestVerificationToken"[^>]+content="([^"]+)"/) ??
      html.match(/__RequestVerificationToken['"]\s*:\s*['"]([^'"]+)['"]/);

    if (!tokenMatch) {
      console.warn("[e3030] Could not extract CSRF token — proceeding without it");
      return { cookies, csrfToken: "" };
    }

    return { cookies, csrfToken: tokenMatch[1] };
  } catch (err) {
    console.error("[e3030] Session acquisition failed:", (err as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// E3030 download
// ---------------------------------------------------------------------------

/**
 * Download permit CSV from the E3030 form endpoint.
 *
 * E3030 uses a POST form with hidden params to select the dataset:
 *   t = dataset class (2 = 建照/使照, 3 = 開工)
 *   k = sub-type (3 = 開工數, 4 = 執照數)
 *   n = field selector (1 = 建照宅數, 3 = 使照宅數, 2 = 開工宅數)
 *
 * Returns raw CSV text or null on failure.
 */
async function downloadE3030Csv(
  session: E3030Session,
  kind: PermitKind,
): Promise<string | null> {
  const params = PERMIT_TYPES[kind];
  const body = new URLSearchParams({
    t: params.t,
    k: params.k,
    n: params.n,
    ...(session.csrfToken ? { __RequestVerificationToken: session.csrfToken } : {}),
  });

  const url = `${BASE_URL}/DownloadCsv`;
  console.log(`[e3030] Downloading ${kind} (t=${params.t},k=${params.k},n=${params.n})…`);

  try {
    const res = await fetchWithRetry(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        ...(session.cookies ? { Cookie: session.cookies } : {}),
      },
      body: body.toString(),
    });
    const text = await res.text();
    if (text.trim().length === 0) {
      console.warn(`[e3030] Empty response for ${kind}`);
      return null;
    }
    return text;
  } catch (err) {
    console.error(`[e3030] Download failed for ${kind}:`, (err as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

interface RawE3030Row {
  quarter: string;        // e.g. "2021Q1"
  city: string;           // normalised city name
  value: number;          // unit count
}

/**
 * Parse E3030 CSV.
 *
 * Expected format (BIG5 or UTF-8, with BOM):
 *   年季,縣市,宅數
 *   1101,台北市,3000
 *   ...
 *
 * 年季 is in ROC "YYYQ" format (3-digit ROC year + quarter digit).
 * e.g. 1101 = ROC 110 Q1 = 2021 Q1
 */
function parseE3030Csv(csv: string): RawE3030Row[] {
  const lines = csv
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  if (lines.length === 0) return [];

  // Find header row — look for 年季 or 年度季別
  const headerIdx = lines.findIndex(l => l.includes("年季") || l.includes("年度") || l.includes("縣市"));
  const dataStart = headerIdx >= 0 ? headerIdx + 1 : 1;

  const rows: RawE3030Row[] = [];

  for (let i = dataStart; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;

    // Split by comma or tab
    const cols = line.includes("\t") ? line.split("\t") : line.split(",");
    if (cols.length < 3) continue;

    const rawQuarter = cols[0].trim().replace(/"/g, "");
    const rawCity    = cols[1].trim().replace(/"/g, "");
    const rawValue   = cols[2].trim().replace(/"/g, "").replace(/,/g, "");

    // Parse ROC quarter: "1101" → 2021Q1
    const quarter = parseRocQuarter(rawQuarter);
    if (!quarter) continue;

    // Normalise city name
    const city = CITY_ALIASES[rawCity];
    if (!city) continue;  // skip non-6-city rows

    const value = parseInt(rawValue, 10);
    if (isNaN(value)) continue;

    rows.push({ quarter, city, value });
  }

  return rows;
}

/** Convert ROC quarter string to AD quarter string. "1101" → "2021Q1" */
function parseRocQuarter(raw: string): string | null {
  // Support formats: "1101", "110Q1", "110-1", "110年第1季"
  const m =
    raw.match(/^(\d{3})(\d)$/) ??            // 1101
    raw.match(/^(\d{3})Q(\d)$/i) ??          // 110Q1
    raw.match(/^(\d{3})-(\d)$/) ??           // 110-1
    raw.match(/^(\d{3})年第(\d)季$/);        // 110年第1季

  if (!m) return null;
  const rocYear = parseInt(m[1], 10);
  const q = parseInt(m[2], 10);
  if (isNaN(rocYear) || isNaN(q) || q < 1 || q > 4) return null;
  const adYear = rocYear + ROC_BASE;
  return `${adYear}Q${q}`;
}

// ---------------------------------------------------------------------------
// Data assembly
// ---------------------------------------------------------------------------

/**
 * Generate quarters list from 2021Q1 to 2028Q4.
 * Historical: 2021Q1–latest actuals (2025Q4 per issue context).
 * Projected: 2026Q1–2028Q4.
 */
function buildQuartersList(from = "2021Q1", to = "2028Q4"): string[] {
  const quarters: string[] = [];
  const [fromYear, fromQ] = parseQuarterParts(from);
  const [toYear, toQ] = parseQuarterParts(to);

  let year = fromYear, q = fromQ;
  while (year < toYear || (year === toYear && q <= toQ)) {
    quarters.push(`${year}Q${q}`);
    q++;
    if (q > 4) { q = 1; year++; }
  }
  return quarters;
}

function parseQuarterParts(q: string): [number, number] {
  const m = q.match(/^(\d{4})Q(\d)$/);
  if (!m) throw new Error(`Invalid quarter: ${q}`);
  return [parseInt(m[1], 10), parseInt(m[2], 10)];
}

/**
 * Build projected occupancy_permits using a simple lag model:
 * - 建照 units from Q-{LAG_MIN} to Q-{LAG_MAX} ago → expected completions
 * - Apply ~75% conversion rate (some projects stall/cancel)
 * - Spread linearly across the projection window
 */
function projectOccupancyPermits(
  allQuarters: string[],
  building_permits: number[],
  first_projected_quarter: string,
): number[] {
  const LAG_MIN = 12; // quarters
  const LAG_MAX = 16;
  const CONVERSION = 0.75;

  const projIdx = allQuarters.indexOf(first_projected_quarter);
  const result: number[] = [...building_permits]; // copy actuals

  for (let i = projIdx; i < allQuarters.length; i++) {
    // Average building_permits from LAG_MIN to LAG_MAX quarters ago
    const samples: number[] = [];
    for (let lag = LAG_MIN; lag <= LAG_MAX; lag++) {
      const srcIdx = i - lag;
      if (srcIdx >= 0 && srcIdx < projIdx) {
        samples.push(building_permits[srcIdx]);
      }
    }
    const avg = samples.length > 0
      ? samples.reduce((a, b) => a + b, 0) / samples.length
      : (result[projIdx - 1] ?? 3000); // fallback: last known value
    result[i] = Math.round(avg * CONVERSION);
  }

  return result;
}

// ---------------------------------------------------------------------------
// Main assembly: merge all permit kinds into PermitsJson
// ---------------------------------------------------------------------------

function assemblePermitsJson(
  rawByKind: Record<PermitKind, RawE3030Row[]>,
  allQuarters: string[],
  firstProjectedQuarter: string,
): PermitsJson {
  // Build lookup: kind → city → quarter → value
  const lookup: Record<PermitKind, Record<string, Record<string, number>>> = {
    building_permits: {},
    occupancy_permits: {},
    starts: {},
  };

  for (const [kind, rows] of Object.entries(rawByKind) as [PermitKind, RawE3030Row[]][]) {
    for (const row of rows) {
      if (!lookup[kind][row.city]) lookup[kind][row.city] = {};
      lookup[kind][row.city][row.quarter] = row.value;
    }
  }

  const cities: PermitsJson["cities"] = {} as PermitsJson["cities"];
  const projIdx = allQuarters.indexOf(firstProjectedQuarter);

  for (const city of SIX_CITIES) {
    const bp = allQuarters.map((q, i) => {
      if (i < projIdx) {
        return lookup.building_permits[city]?.[q] ?? 0;
      }
      // For projection quarters, keep building_permits as 0 (not projected)
      return 0;
    });

    // For occupancy_permits: use actuals where available, project the rest
    const op = projectOccupancyPermits(allQuarters, bp, firstProjectedQuarter);
    // Use actual occupancy values for historical quarters
    for (let i = 0; i < projIdx; i++) {
      op[i] = lookup.occupancy_permits[city]?.[allQuarters[i]] ?? op[i];
    }

    const st = allQuarters.map((q, i) => {
      if (i < projIdx) {
        return lookup.starts[city]?.[q] ?? 0;
      }
      return 0;
    });

    cities[city] = { building_permits: bp, occupancy_permits: op, starts: st };
  }

  return {
    _comment: "內政部不動產資訊平台 E3030 解析資料。2026-2028 為模型推估預測值。",
    generated: new Date().toISOString().slice(0, 10),
    source: "內政部不動產資訊平台 E3030（建照/使照/開工，縣市層級）",
    note: "2026-2028 為依建照核發量推估之預測值（建照到使照平均時差約12-16季，轉換率約75%）。",
    quarters: allQuarters,
    first_projected_quarter: firstProjectedQuarter,
    cities,
  };
}

// ---------------------------------------------------------------------------
// DB persistence
// ---------------------------------------------------------------------------

async function ensurePermitsTable(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS permit_records (
      id SERIAL PRIMARY KEY,
      city TEXT NOT NULL,
      district TEXT,
      quarter TEXT NOT NULL,               -- e.g. '2024Q1'
      quarter_year INTEGER NOT NULL,       -- AD year
      quarter_num SMALLINT NOT NULL,       -- 1-4
      building_permits INTEGER NOT NULL DEFAULT 0,
      occupancy_permits INTEGER NOT NULL DEFAULT 0,
      starts INTEGER NOT NULL DEFAULT 0,
      is_projected BOOLEAN NOT NULL DEFAULT FALSE,
      source TEXT,
      imported_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(city, COALESCE(district, ''), quarter)
    );
    CREATE INDEX IF NOT EXISTS idx_permit_records_city_quarter
      ON permit_records(city, quarter);
  `);
}

async function upsertPermitRecords(records: PermitRecord[], isProjected = false): Promise<number> {
  let upserted = 0;
  for (const rec of records) {
    const [year, q] = parseQuarterParts(rec.quarter);
    await query(
      `INSERT INTO permit_records
         (city, district, quarter, quarter_year, quarter_num,
          building_permits, occupancy_permits, starts, is_projected, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT (city, COALESCE(district, ''), quarter)
       DO UPDATE SET
         building_permits = EXCLUDED.building_permits,
         occupancy_permits = EXCLUDED.occupancy_permits,
         starts = EXCLUDED.starts,
         is_projected = EXCLUDED.is_projected,
         imported_at = NOW()`,
      [
        rec.city, rec.district ?? null, rec.quarter, year, q,
        rec.building_permits, rec.occupancy_permits, rec.starts,
        isProjected, "pip.moi.gov.tw/E3030",
      ]
    );
    upserted++;
  }
  return upserted;
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const isDryRun = args.includes("--dry-run");
  const outputPath = (() => {
    const i = args.indexOf("--output");
    return i >= 0 ? args[i + 1] : null;
  })();

  const allQuarters = buildQuartersList("2021Q1", "2028Q4");
  const firstProjectedQuarter = "2026Q1";

  console.log("🏗️  E3030 Permit Ingest — 內政部不動產資訊平台");
  console.log(`   Quarters: ${allQuarters[0]} → ${allQuarters[allQuarters.length - 1]}`);
  console.log(`   First projected quarter: ${firstProjectedQuarter}`);

  // ── Step 1: acquire session ────────────────────────────────────────────
  console.log("\n[1/4] Acquiring E3030 session…");
  const session = await acquireE3030Session();

  // ── Step 2: download all 3 permit kinds ───────────────────────────────
  console.log("\n[2/4] Downloading permit data…");
  const rawByKind: Partial<Record<PermitKind, RawE3030Row[]>> = {};

  if (session) {
    for (const kind of Object.keys(PERMIT_TYPES) as PermitKind[]) {
      await sleep(500); // polite crawl delay
      const csv = await downloadE3030Csv(session, kind);
      if (csv) {
        const rows = parseE3030Csv(csv);
        console.log(`   ${kind}: ${rows.length} rows parsed`);
        rawByKind[kind] = rows;
      } else {
        console.warn(`   ${kind}: download failed — using empty data`);
        rawByKind[kind] = [];
      }
    }
  } else {
    console.warn("   Session unavailable — using empty data for all kinds");
    for (const kind of Object.keys(PERMIT_TYPES) as PermitKind[]) {
      rawByKind[kind] = [];
    }
  }

  // ── Step 3: assemble PermitsJson ──────────────────────────────────────
  console.log("\n[3/4] Assembling permits JSON…");
  const permitsJson = assemblePermitsJson(
    rawByKind as Record<PermitKind, RawE3030Row[]>,
    allQuarters,
    firstProjectedQuarter,
  );

  // ── Step 4: persist / output ──────────────────────────────────────────
  if (outputPath) {
    await mkdir(path.dirname(outputPath), { recursive: true });
    await writeFile(outputPath, JSON.stringify(permitsJson, null, 2), "utf-8");
    console.log(`\n✅ Written to ${outputPath}`);
  }

  if (!isDryRun) {
    console.log("\n[4/4] Writing to database…");
    await ensurePermitsTable();

    // Flatten PermitsJson → PermitRecord[] for DB upsert
    const records: PermitRecord[] = [];
    for (const city of SIX_CITIES) {
      const cityData = permitsJson.cities[city];
      const projIdx = allQuarters.indexOf(firstProjectedQuarter);
      for (let i = 0; i < allQuarters.length; i++) {
        records.push({
          city,
          district: null,
          quarter: allQuarters[i],
          building_permits: cityData.building_permits[i],
          occupancy_permits: cityData.occupancy_permits[i],
          starts: cityData.starts[i],
        });
      }
    }

    const histRecs = records.filter(r => {
      const idx = allQuarters.indexOf(r.quarter);
      return idx < allQuarters.indexOf(firstProjectedQuarter);
    });
    const projRecs = records.filter(r => {
      const idx = allQuarters.indexOf(r.quarter);
      return idx >= allQuarters.indexOf(firstProjectedQuarter);
    });

    const histCount = await upsertPermitRecords(histRecs, false);
    const projCount = await upsertPermitRecords(projRecs, true);
    console.log(`   Upserted ${histCount} historical + ${projCount} projected records`);
    await close();
  } else {
    console.log("\n[4/4] Dry run — skipping database write");
  }

  console.log("\n✅ Done.");
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
