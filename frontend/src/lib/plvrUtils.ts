/**
 * Pure utility functions for PLVR (實價登錄) data parsing.
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Handles the two critical parsing traps from Taiwan gov open data:
 *  1. Full-width (CJK) digits in addresses/fields (U+FF10–FF19) must be
 *     NFKC-normalized before any regex or numeric parsing.
 *  2. ROC/民國 date integers (e.g. 741024 = 民國74年10月24日 = 1985-10-24):
 *     split YYMMDD or YYYMMDD, add 1911 for Gregorian year.
 *     Treating 741024 as a calendar year gives garbage.
 *
 * Column-layout guard:
 *  - Legacy 26-col format: pre-2025 per-city seasonal CSVs
 *  - New 28-col format: 2025+ bulk national ZIP (adds 非都市土地使用分區 +
 *    非都市土地使用編定 after 都市土地使用分區)
 */

// ── Constants ─────────────────────────────────────────────────────────────────

/** Column counts for the two known PLVR CSV schemas */
export const PLVR_COL_COUNT_LEGACY = 26;
export const PLVR_COL_COUNT_NEW = 28;

/** Minimum column count: reject rows narrower than this */
export const PLVR_COL_MIN = PLVR_COL_COUNT_LEGACY;

/** Header token that distinguishes the new 28-col bulk format */
export const PLVR_NEW_FORMAT_MARKER = "非都市土地使用分區";

// ── NFKC normalization ────────────────────────────────────────────────────────

/**
 * Normalize a string with NFKC Unicode normalization.
 * Converts full-width (CJK) digits (U+FF10–FF19) and other compatibility
 * characters to their ASCII equivalents before any regex/numeric parsing.
 *
 * @example
 *   normalizeFullWidth("４３號")  // → "43號"
 *   normalizeFullWidth("７４１０２４")  // → "741024"
 */
export function normalizeFullWidth(str: string): string {
  return str.normalize("NFKC");
}

// ── ROC / 民國 date parsing ───────────────────────────────────────────────────

/**
 * Parse a ROC transaction date string (交易年月日 field) to an ISO date string.
 *
 * PLVR format: last 4 chars = MMDD, leading chars = ROC year (民國年).
 * Examples: "1130115" → "2024-01-15", "741024" → "1985-10-24"
 *
 * Applies NFKC normalization first so full-width digit strings are handled.
 *
 * @returns ISO date string "YYYY-MM-DD", or null if unparseable.
 */
export function parseRocDate(rocDateStr: string | null | undefined): string | null {
  if (!rocDateStr) return null;
  const normalized = normalizeFullWidth(String(rocDateStr)).trim();
  if (!/^\d+$/.test(normalized) || normalized.length < 5) return null;

  // Last 4 digits = MMDD, everything before = ROC year
  const rocYear = parseInt(normalized.slice(0, normalized.length - 4), 10);
  const month = parseInt(normalized.slice(-4, -2), 10);
  const day = parseInt(normalized.slice(-2), 10);

  if (isNaN(rocYear) || isNaN(month) || isNaN(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  const year = rocYear + 1911;
  if (year < 1945 || year > 2100) return null; // sanity bounds

  // Validate the date is real (e.g. reject Feb 31 → would silently roll over to Mar 2)
  const d = new Date(year, month - 1, day);
  if (d.getFullYear() !== year || d.getMonth() + 1 !== month || d.getDate() !== day) return null;

  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Parse a ROC building completion year (建築完成年月 field) to a Gregorian year.
 *
 * PLVR field is a 6-digit (YYMMDD) or 7-digit (YYYMMDD) integer, or a string
 * thereof (possibly with full-width digits). Extracts only the year component.
 *
 * Handles all known forms:
 *  - 741024  (6-digit YYMMDD: ROC 74/10/24 → 1985)
 *  - 1081128 (7-digit YYYMMDD: ROC 108/11/28 → 2019)
 *  - 74      (plain ROC year < 200 → add 1911)
 *  - 1985    (already Gregorian year ≥ 1900)
 *  - "７４１０２４" (full-width digits → NFKC-normalized first)
 *
 * @param maxYear Clock-independent ceiling; defaults to current year + 1.
 *                Set in tests for determinism.
 * @returns Gregorian year integer, or null if unparseable / out of range.
 */
export function parseRocBuildYear(
  value: number | string | null | undefined,
  maxYear: number = new Date().getFullYear() + 1,
): number | null {
  if (value === null || value === undefined || value === "") return null;

  let str: string;
  if (typeof value === "number") {
    if (!Number.isInteger(value) || value <= 0) return null;
    str = String(value);
  } else {
    str = normalizeFullWidth(String(value)).trim();
    if (!/^\d+$/.test(str)) return null;
  }

  const n = parseInt(str, 10);
  if (isNaN(n) || n <= 0) return null;

  let year: number;

  if (str.length === 7) {
    // YYYMMDD: ROC 3-digit year + MMDD, e.g. 1081128
    year = Math.floor(n / 10000) + 1911;
  } else if (str.length === 6) {
    // YYMMDD: ROC 2-digit year + MMDD, e.g. 741024
    year = Math.floor(n / 10000) + 1911;
  } else if (str.length <= 3 && n < 200) {
    // Plain ROC year (e.g. 74 → 1985, 100 → 2011)
    year = n + 1911;
  } else if (str.length === 4 && n >= 1900) {
    // Already a Gregorian year (e.g. 1985, 2010)
    year = n;
  } else {
    return null;
  }

  if (year < 1900 || year > maxYear) return null;
  return year;
}

// ── CSV format detection ──────────────────────────────────────────────────────

/**
 * Detect whether a PLVR CSV header line is the new 28-column bulk format
 * (2025+) or the legacy 26-column per-city format.
 *
 * The new format inserts 非都市土地使用分區 and 非都市土地使用編定 after
 * 都市土地使用分區 (positions 5–6 in 0-indexed order).
 */
export function detectCsvFormat(headerLine: string): "new28" | "legacy26" {
  const normalized = normalizeFullWidth(headerLine);
  return normalized.includes(PLVR_NEW_FORMAT_MARKER) ? "new28" : "legacy26";
}

/**
 * Validate that a parsed row has at least the minimum expected number of columns.
 * Rows shorter than minCols are likely malformed or truncated and should be skipped.
 */
export function validateColumnCount(cols: string[], minCols = PLVR_COL_MIN): boolean {
  return cols.length >= minCols;
}
