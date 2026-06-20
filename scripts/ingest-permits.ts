#!/usr/bin/env tsx
/**
 * Ingest permit data from \u5167\u653f\u90e8\u4e0d\u52d5\u7522\u8cc7\u8a0a\u5e73\u53f0 E3030
 * Source: pip.moi.gov.tw/Publicize/Info/E3030
 * Operator: \u5167\u653f\u90e8\u570b\u571f\u7ba1\u7406\u7f72
 * License: \u653f\u5e9c\u8cc7\u6599\u958b\u653e\u6388\u6b0a\u689d\u6b3e-\u7b2c1\u7248 (CC-BY equivalent)
 *
 * Fields fetched:
 *   \u5efa\u9020\u57f7\u7167\u7e3d\u5b85\u6578 (building_permits)  t=2,k=4,n=1
 *   \u4f7f\u7528\u57f7\u7167\u7e3d\u5b85\u6578 (occupancy_permits) t=2,k=4,n=3
 *   \u5efa\u7269\u958b\u5de5\u7e3d\u5b85\u6578 (starts)            t=2,k=3,n=2
 *
 * Granularity: \u7e23\u5e02 (city level). Quarterly updates from E3030.
 *
 * E3030 CSV exports may be Big5 encoded -- charset is detected from the
 * Content-Type response header; iconv-lite is used for Big5 decoding.
 *
 * Usage (run from project root):
 *   tsx scripts/ingest-permits.ts [--dry-run] [--output path]
 *   --dry-run   Fetch and parse but do not write to DB
 *   --output    Write assembled JSON (for seeding frontend/public/data/permits.json)
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { query, close } from "./backend/src/db/connection.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BASE_URL = "https://pip.moi.gov.tw/Publicize/Info/E3030";
const ROC_BASE = 1911;

const PERMIT_TYPES = {
  building_permits:  { t: "2", k: "4", n: "1" }, // \u5efa\u9020\u57f7\u7167\u7e3d\u5b85\u6578
  occupancy_permits: { t: "2", k: "4", n: "3" }, // \u4f7f\u7528\u57f7\u7167\u7e3d\u5b85\u6578
  starts:            { t: "2", k: "3", n: "2" }, // \u5efa\u7269\u958b\u5de5\u7e3d\u5b85\u6578
} as const;

type PermitKind = keyof typeof PERMIT_TYPES;

/** City name normalisation (\u81fa vs \u53f0 variants). */
const CITY_ALIASES: Record<string, string> = {
  "\u81fa\u5317\u5e02": "\u53f0\u5317\u5e02",
  "\u53f0\u5317\u5e02": "\u53f0\u5317\u5e02",
  "\u65b0\u5317\u5e02": "\u65b0\u5317\u5e02",
  "\u6843\u5712\u5e02": "\u6843\u5712\u5e02",
  "\u81fa\u4e2d\u5e02": "\u53f0\u4e2d\u5e02",
  "\u53f0\u4e2d\u5e02": "\u53f0\u4e2d\u5e02",
  "\u81fa\u5357\u5e02": "\u53f0\u5357\u5e02",
  "\u53f0\u5357\u5e02": "\u53f0\u5357\u5e02",
  "\u9ad8\u96c4\u5e02": "\u9ad8\u96c4\u5e02",
};

const SIX_CITIES = [
  "\u53f0\u5317\u5e02",
  "\u65b0\u5317\u5e02",
  "\u6843\u5712\u5e02",
  "\u53f0\u4e2d\u5e02",
  "\u53f0\u5357\u5e02",
  "\u9ad8\u96c4\u5e02",
] as const satisfies string[];

type City = typeof SIX_CITIES[number];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PermitRecord {
  city: string;
  district: string;   // '' = city-level aggregate (matches NOT NULL DEFAULT '' in DB)
  quarter: string;    // e.g. "2024Q1"
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

async function fetchWithRetry(url: string, opts: RequestInit = {}, retries = 3): Promise<Response> {
  const headers: Record<string, string> = {
    "User-Agent": "RealEstateRadar/0.1 (github.com/copilot-autogent/realestate-radar; data research)",
    "Accept": "text/csv,text/html,application/xhtml+xml,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://pip.moi.gov.tw/Publicize/Info/E3030",
    ...(opts.headers as Record<string, string> ?? {}),
  };
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { ...opts, headers, signal: AbortSignal.timeout(30_000) });
      if (res.ok) return res;
      if (res.status === 429 || res.status >= 500) {
        await sleep(2000 * (attempt + 1));
        continue;
      }
      throw new Error(\`HTTP \${res.status} \${res.statusText}\`);
    } catch (err) {
      if (attempt === retries) throw err;
      console.warn(\`[retry] \${(err as Error).message}\`);
      await sleep(2000 * (attempt + 1));
    }
  }
  throw new Error("Unreachable");
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Decode HTTP response body, handling Big5 charset from Content-Type.
 * Falls back to UTF-8 when charset is absent or unrecognised.
 */
async function decodeResponseBody(res: Response): Promise<string> {
  const contentType = res.headers.get("content-type") ?? "";
  const charsetMatch = contentType.match(/charset\s*=\s*([^\s;]+)/i);
  const charset = charsetMatch ? charsetMatch[1]!.toLowerCase() : "utf-8";
  const bytes = Buffer.from(await res.arrayBuffer());
  if (charset === "big5" || charset === "big5-hkscs" || charset === "csbig5") {
    try {
      const iconv = await import("iconv-lite");
      return iconv.decode(bytes, "big5");
    } catch {
      console.warn("[encode] iconv-lite unavailable; decoding Big5 as UTF-8 (may mojibake)");
    }
  }
  const text = new TextDecoder("utf-8").decode(bytes);
  return text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
}

// ---------------------------------------------------------------------------
// E3030 session & CSRF token acquisition
// ---------------------------------------------------------------------------

interface E3030Session {
  cookieHeader: string;  // parsed "name=value; name2=value2" pairs
  csrfToken: string;
}

/**
 * Parse raw Set-Cookie header into a Cookie header string.
 * Extracts only name=value pairs, discarding Path/Expires/HttpOnly attributes.
 */
function parseCookies(rawSetCookie: string | null): string {
  if (!rawSetCookie) return "";
  const parts = rawSetCookie.split(/,\s*(?=[A-Za-z_][^=]+=)/);
  return parts.map(p => (p.split(";")[0] ?? "").trim()).filter(Boolean).join("; ");
}

async function acquireE3030Session(): Promise<E3030Session | null> {
  try {
    const res = await fetchWithRetry(BASE_URL);
    const cookieHeader = parseCookies(res.headers.get("set-cookie"));
    const html = await decodeResponseBody(res);
    const tokenMatch =
      html.match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/) ??
      html.match(/<meta[^>]+name="__RequestVerificationToken"[^>]+content="([^"]+)"/) ??
      html.match(/__RequestVerificationToken['"]\s*:\s*['"]([^'"]+)['"]/);
    if (!tokenMatch) {
      console.warn("[e3030] Could not extract CSRF token -- proceeding without it");
      return { cookieHeader, csrfToken: "" };
    }
    return { cookieHeader, csrfToken: tokenMatch[1]! };
  } catch (err) {
    console.error("[e3030] Session acquisition failed:", (err as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// E3030 CSV download
// ---------------------------------------------------------------------------

async function downloadE3030Csv(session: E3030Session, kind: PermitKind): Promise<string | null> {
  const params = PERMIT_TYPES[kind];
  const bodyParams = new URLSearchParams({
    t: params.t, k: params.k, n: params.n,
    ...(session.csrfToken ? { __RequestVerificationToken: session.csrfToken } : {}),
  });
  console.log(\`[e3030] Downloading \${kind} (t=\${params.t},k=\${params.k},n=\${params.n})...\`);
  try {
    const res = await fetchWithRetry(\`\${BASE_URL}/DownloadCsv\`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        ...(session.cookieHeader ? { Cookie: session.cookieHeader } : {}),
      },
      body: bodyParams.toString(),
    });
    const text = await decodeResponseBody(res);
    if (!text.trim()) { console.warn(\`[e3030] Empty response for \${kind}\`); return null; }
    return text;
  } catch (err) {
    console.error(\`[e3030] Download failed for \${kind}:\`, (err as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

interface RawE3030Row {
  quarter: string;
  city: string;
  value: number;
}

/** Split a CSV line by comma, respecting double-quoted fields. */
function splitCsvLine(line: string): string[] {
  const cols: string[] = [];
  let inQuote = false, cur = "";
  for (const ch of line) {
    if (ch === '"') inQuote = !inQuote;
    else if (ch === "," && !inQuote) { cols.push(cur); cur = ""; }
    else cur += ch;
  }
  cols.push(cur);
  return cols;
}

/**
 * Parse E3030 CSV (UTF-8 or Big5, optional BOM).
 *
 * Expected format:
 *   \u5e74\u5b63,\u7e23\u5e02,\u5b85\u6578
 *   1101,\u53f0\u5317\u5e02,3000
 *
 * Year-quarter is ROC "YYYQ" format: 1101 = ROC 110 Q1 = 2021 Q1.
 */
function parseE3030Csv(csv: string): RawE3030Row[] {
  const lines = csv.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
    .split("\n").map(l => l.trim()).filter(Boolean);
  if (!lines.length) return [];

  const headerIdx = lines.findIndex(l =>
    l.includes("\u5e74\u5b63") || l.includes("\u5e74\u5ea6") || l.includes("\u7e23\u5e02")
  );
  const dataStart = headerIdx >= 0 ? headerIdx + 1 : 1;
  const rows: RawE3030Row[] = [];

  for (let i = dataStart; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = line.includes("\t") ? line.split("\t") : splitCsvLine(line);
    if (cols.length < 3) continue;

    const rawQuarter = (cols[0] ?? "").trim().replace(/"/g, "");
    const rawCity    = (cols[1] ?? "").trim().replace(/"/g, "");
    const rawValue   = (cols[2] ?? "").trim().replace(/"/g, "").replace(/,/g, "");

    const quarter = parseRocQuarter(rawQuarter);
    if (!quarter) continue;
    const city = CITY_ALIASES[rawCity];
    if (!city) continue;
    const value = parseInt(rawValue, 10);
    if (isNaN(value)) continue;
    rows.push({ quarter, city, value });
  }
  return rows;
}

/** Convert ROC quarter string to AD quarter string: "1101" -> "2021Q1" */
function parseRocQuarter(raw: string): string | null {
  const m =
    raw.match(/^(\d{3})(\d)$/) ??
    raw.match(/^(\d{3})Q(\d)$/i) ??
    raw.match(/^(\d{3})-(\d)$/) ??
    raw.match(/^(\d{3})\u5e74\u7b2c(\d)\u5b63$/);
  if (!m) return null;
  const rocYear = parseInt(m[1]!, 10), q = parseInt(m[2]!, 10);
  if (isNaN(rocYear) || isNaN(q) || q < 1 || q > 4) return null;
  return \`\${rocYear + ROC_BASE}Q\${q}\`;
}

// ---------------------------------------------------------------------------
// Data assembly
// ---------------------------------------------------------------------------

function buildQuartersList(from = "2021Q1", to = "2028Q4"): string[] {
  const quarters: string[] = [];
  const [fy, fq] = parseQuarterParts(from), [ty, tq] = parseQuarterParts(to);
  let y = fy, q = fq;
  while (y < ty || (y === ty && q <= tq)) {
    quarters.push(\`\${y}Q\${q}\`);
    if (++q > 4) { q = 1; y++; }
  }
  return quarters;
}

function parseQuarterParts(q: string): [number, number] {
  const m = q.match(/^(\d{4})Q(\d)$/);
  if (!m) throw new Error(\`Invalid quarter: \${q}\`);
  return [parseInt(m[1]!, 10), parseInt(m[2]!, 10)];
}

/**
 * Project 2026-2028 \u4f7f\u7167 from \u5efa\u7167 using lag model (12-16 quarter lag, 75% conversion).
 * historicalOp: actual occupancy_permits for historical quarters, zeros elsewhere.
 */
function projectOccupancyPermits(
  allQuarters: string[],
  bp: number[],
  historicalOp: number[],
  firstProjectedQuarter: string,
): number[] {
  const LAG_MIN = 12, LAG_MAX = 16, CONVERSION = 0.75;
  const projIdx = allQuarters.indexOf(firstProjectedQuarter);
  if (projIdx < 0) return [...historicalOp]; // guard: quarter not in list
  const result = [...historicalOp];
  for (let i = projIdx; i < allQuarters.length; i++) {
    const samples: number[] = [];
    for (let lag = LAG_MIN; lag <= LAG_MAX; lag++) {
      const srcIdx = i - lag;
      if (srcIdx >= 0 && srcIdx < projIdx) samples.push(bp[srcIdx] ?? 0);
    }
    const avg = samples.length > 0
      ? samples.reduce((a, b) => a + b, 0) / samples.length
      : (historicalOp[projIdx - 1] ?? 0); // fallback: last known occupancy value
    result[i] = Math.round(avg * CONVERSION);
  }
  return result;
}

function assemblePermitsJson(
  rawByKind: Record<PermitKind, RawE3030Row[]>,
  allQuarters: string[],
  firstProjectedQuarter: string,
): PermitsJson {
  type CityLookup = Record<string, Record<string, number>>;
  const lookup: Record<PermitKind, CityLookup> = {
    building_permits: {}, occupancy_permits: {}, starts: {},
  };
  for (const [kind, rows] of Object.entries(rawByKind) as [PermitKind, RawE3030Row[]][]) {
    for (const row of rows) {
      if (!lookup[kind][row.city]) lookup[kind][row.city] = {};
      lookup[kind][row.city]![row.quarter] = row.value;
    }
  }

  const cities: PermitsJson["cities"] = {} as PermitsJson["cities"];
  const projIdx = allQuarters.indexOf(firstProjectedQuarter);

  for (const city of SIX_CITIES) {
    const bp: number[] = allQuarters.map((q, i) =>
      i < projIdx ? (lookup.building_permits[city]?.[q] ?? 0) : 0
    );
    const historicalOp: number[] = allQuarters.map((q, i) =>
      i < projIdx ? (lookup.occupancy_permits[city]?.[q] ?? 0) : 0
    );
    const op = projectOccupancyPermits(allQuarters, bp, historicalOp, firstProjectedQuarter);
    const st: number[] = allQuarters.map((q, i) =>
      i < projIdx ? (lookup.starts[city]?.[q] ?? 0) : 0
    );
    cities[city] = { building_permits: bp, occupancy_permits: op, starts: st };
  }

  return {
    _comment: "\u5167\u653f\u90e8\u4e0d\u52d5\u7522\u8cc7\u8a0a\u5e73\u53f0 E3030 \u89e3\u6790\u8cc7\u6599\u30022026-2028 \u70ba\u6a21\u578b\u63a8\u4f30\u9810\u6e2c\u5024\u3002",
    generated: new Date().toISOString().slice(0, 10),
    source: "\u5167\u653f\u90e8\u4e0d\u52d5\u7522\u8cc7\u8a0a\u5e73\u53f0 E3030\uff08\u5efa\u7167/\u4f7f\u7167/\u958b\u5de5\uff0c\u7e23\u5e02\u5c64\u7d1a\uff09",
    note: "2026-2028 \u70ba\u4f9d\u5efa\u7167\u63a8\u4f30\u9810\u6e2c\u5024\uff0812-16\u5b63\u6642\u5dee\uff0c\u8f49\u63db\u7387\u7d0475%\uff09\u3002",
    quarters: allQuarters,
    first_projected_quarter: firstProjectedQuarter,
    cities,
  };
}

// ---------------------------------------------------------------------------
// DB persistence
// ---------------------------------------------------------------------------

async function ensurePermitsTable(): Promise<void> {
  await query(\`
    CREATE TABLE IF NOT EXISTS permit_records (
      id SERIAL PRIMARY KEY,
      city TEXT NOT NULL,
      district TEXT NOT NULL DEFAULT '',
      quarter TEXT NOT NULL,
      quarter_year INTEGER NOT NULL,
      quarter_num SMALLINT NOT NULL,
      building_permits INTEGER NOT NULL DEFAULT 0,
      occupancy_permits INTEGER NOT NULL DEFAULT 0,
      starts INTEGER NOT NULL DEFAULT 0,
      is_projected BOOLEAN NOT NULL DEFAULT FALSE,
      source TEXT,
      imported_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(city, district, quarter)
    );
    CREATE INDEX IF NOT EXISTS idx_permit_records_city_quarter
      ON permit_records(city, quarter);
    CREATE INDEX IF NOT EXISTS idx_permit_records_quarter_year
      ON permit_records(quarter_year, quarter_num);
  \`);
}

async function upsertPermitRecords(records: PermitRecord[], isProjected: boolean): Promise<number> {
  if (!records.length) return 0;
  await query("BEGIN");
  try {
    for (const rec of records) {
      const [year, q] = parseQuarterParts(rec.quarter);
      await query(
        \`INSERT INTO permit_records
           (city, district, quarter, quarter_year, quarter_num,
            building_permits, occupancy_permits, starts, is_projected, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (city, district, quarter)
         DO UPDATE SET
           building_permits  = EXCLUDED.building_permits,
           occupancy_permits = EXCLUDED.occupancy_permits,
           starts            = EXCLUDED.starts,
           is_projected      = EXCLUDED.is_projected,
           imported_at       = NOW()\`,
        [rec.city, rec.district, rec.quarter, year, q,
         rec.building_permits, rec.occupancy_permits, rec.starts,
         isProjected, "pip.moi.gov.tw/E3030"]
      );
    }
    await query("COMMIT");
  } catch (err) {
    await query("ROLLBACK");
    throw err;
  }
  return records.length;
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const isDryRun = args.includes("--dry-run");
  const outputIdx = args.indexOf("--output");
  const outputPath: string | null =
    outputIdx >= 0 && outputIdx + 1 < args.length ? (args[outputIdx + 1] ?? null) : null;

  const allQuarters = buildQuartersList("2021Q1", "2028Q4");
  const firstProjectedQuarter = "2026Q1";

  console.log("E3030 Permit Ingest -- \u5167\u653f\u90e8\u4e0d\u52d5\u7522\u8cc7\u8a0a\u5e73\u53f0");
  console.log(\`   Quarters: \${allQuarters[0]} -> \${allQuarters[allQuarters.length - 1]}\`);
  if (isDryRun) console.log("   Mode: dry-run (no DB writes)");

  console.log("\\n[1/4] Acquiring E3030 session...");
  const session = await acquireE3030Session();

  console.log("\\n[2/4] Downloading permit data...");
  const rawByKind: Partial<Record<PermitKind, RawE3030Row[]>> = {};
  let anyDownloadSucceeded = false;

  if (session) {
    for (const kind of Object.keys(PERMIT_TYPES) as PermitKind[]) {
      await sleep(800);
      const csv = await downloadE3030Csv(session, kind);
      if (csv) {
        const rows = parseE3030Csv(csv);
        console.log(\`   \${kind}: \${rows.length} rows\`);
        rawByKind[kind] = rows;
        if (rows.length > 0) anyDownloadSucceeded = true;
      } else {
        console.warn(\`   \${kind}: download failed\`);
        rawByKind[kind] = [];
      }
    }
  } else {
    console.warn("   Session unavailable");
    for (const kind of Object.keys(PERMIT_TYPES) as PermitKind[]) rawByKind[kind] = [];
  }

  console.log("\\n[3/4] Assembling permits JSON...");
  const permitsJson = assemblePermitsJson(
    rawByKind as Record<PermitKind, RawE3030Row[]>,
    allQuarters,
    firstProjectedQuarter,
  );

  if (outputPath) {
    await mkdir(path.dirname(path.resolve(outputPath)), { recursive: true });
    await writeFile(outputPath, JSON.stringify(permitsJson, null, 2), "utf-8");
    console.log(\`\\nWritten to \${outputPath}\`);
  }

  if (!isDryRun) {
    // Refuse to overwrite existing DB data when all downloads fail
    if (!anyDownloadSucceeded) {
      console.error("\\nAll permit downloads failed; refusing to overwrite DB with zero data.");
      process.exit(1);
    }
    console.log("\\n[4/4] Writing to database...");
    try {
      await ensurePermitsTable();
      const projIdx = allQuarters.indexOf(firstProjectedQuarter);
      const histRecs: PermitRecord[] = [], projRecs: PermitRecord[] = [];
      for (const city of SIX_CITIES) {
        const cityData = permitsJson.cities[city]!;
        for (let i = 0; i < allQuarters.length; i++) {
          const rec: PermitRecord = {
            city, district: "", quarter: allQuarters[i]!,
            building_permits:  cityData.building_permits[i]  ?? 0,
            occupancy_permits: cityData.occupancy_permits[i] ?? 0,
            starts:            cityData.starts[i]            ?? 0,
          };
          (i < projIdx ? histRecs : projRecs).push(rec);
        }
      }
      const h = await upsertPermitRecords(histRecs, false);
      const p = await upsertPermitRecords(projRecs, true);
      console.log(\`   Upserted \${h} historical + \${p} projected records\`);
    } finally {
      await close();
    }
  } else {
    console.log("\\n[4/4] Dry run -- skipping database write");
  }

  console.log("\\nDone.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
