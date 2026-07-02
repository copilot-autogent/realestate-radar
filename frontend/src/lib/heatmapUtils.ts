/**
 * Pure utility functions for the monthly transaction volume calendar heatmap (issue #112).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export interface HeatmapCell {
  year: number;
  month: number;   // 1–12
  count: number;
  medianUnitPriceWan: number | null;   // NT$ 萬/坪, null when no priced transactions
  isBuyerAdvantaged: boolean;          // count < 70% of that year's peak month
}

export interface HeatmapData {
  district: string;
  isGlobal: boolean;
  cells: HeatmapCell[];
  years: number[];      // sorted ascending
  sufficient: boolean;  // false when < 12 distinct (year, month) pairs
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[mid - 1]! + s[mid]!) / 2 : s[mid]!;
}

/**
 * Parse YYYY-MM-DD (or YYYY/MM/DD) from a property date string.
 * Normalises full-width digits with NFKC before parsing.
 * Returns null on any parse failure.
 */
function parseDateKey(raw: unknown): { year: number; month: number } | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  const m = /^(\d{4})[-/](\d{1,2})/.exec(s);
  if (!m) return null;
  const year = parseInt(m[1]!, 10);
  const month = parseInt(m[2]!, 10);
  if (isNaN(year) || isNaN(month) || month < 1 || month > 12) return null;
  return { year, month };
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Computes monthly transaction heatmap data for a given district (or all
 * districts when `district` is null / `isGlobal` is true).
 *
 * @param features  GeoJSON feature array (rawAllFeatures from Map.astro)
 * @param district  District name to filter, or null for global aggregate
 * @param city      Optional city filter (applied only when district is set)
 * @param maxYears  Number of trailing calendar years to include (default 3)
 */
export function computeHeatmapData(
  features: any[],
  district: string | null,
  city?: string,
  maxYears = 3
): HeatmapData {
  const isGlobal = !district;
  const label = district ?? "全臺灣";

  // Collect (year, month) → { count, unitPrices[] }
  const cellMap = new Map<string, { count: number; prices: number[] }>();

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;
    if (!isGlobal) {
      if (p.district !== district) continue;
      if (city && p.city !== city) continue;
    }
    const parsed = parseDateKey(p.date);
    if (!parsed) continue;
    const key = `${parsed.year}-${String(parsed.month).padStart(2, "0")}`;
    const cell = cellMap.get(key) ?? { count: 0, prices: [] };
    cell.count += 1;
    const up = Number(p.unitPrice);
    if (up > 0) cell.prices.push(up);
    cellMap.set(key, cell);
  }

  if (cellMap.size < 12) {
    return { district: label, isGlobal, cells: [], years: [], sufficient: false };
  }

  // Determine trailing years from the latest date present in data
  // Anchor to the latest (year, month) pair present in data — avoids manufacturing
  // zero-count "future" cells when the newest data doesn't reach December.
  const allKeys = [...cellMap.keys()].sort(); // "YYYY-MM" lexicographic sort
  if (allKeys.length === 0) {
    return { district: label, isGlobal, cells: [], years: [], sufficient: false };
  }
  const latestKey   = allKeys[allKeys.length - 1]!;
  const latestYear  = parseInt(latestKey.slice(0, 4), 10);
  const latestMonth = parseInt(latestKey.slice(5, 7), 10);

  // Build a window of maxYears trailing calendar years, but cap months in the
  // latest year to [1..latestMonth] so we don't emit empty future months.
  const firstYear = latestYear - maxYears + 1;
  const years: number[] = [];
  for (let y = firstYear; y <= latestYear; y++) years.push(y);

  // Build cells for each (year, month) within the window
  const cells: HeatmapCell[] = [];
  for (const year of years) {
    const maxMo = year === latestYear ? latestMonth : 12;
    // Counts per month this year (for buyer-advantaged threshold)
    const monthCounts: number[] = Array(12).fill(0);
    for (let mo = 1; mo <= maxMo; mo++) {
      const key = `${year}-${String(mo).padStart(2, "0")}`;
      monthCounts[mo - 1] = cellMap.get(key)?.count ?? 0;
    }
    const yearPeak = Math.max(...monthCounts.slice(0, maxMo));

    for (let mo = 1; mo <= maxMo; mo++) {
      const key = `${year}-${String(mo).padStart(2, "0")}`;
      const entry = cellMap.get(key);
      const count = entry?.count ?? 0;
      const prices = entry?.prices ?? [];
      const medianUnitPriceWan =
        prices.length > 0 ? Math.round(median(prices) / 10000 * 10) / 10 : null;
      const isBuyerAdvantaged = yearPeak > 0 && count > 0 && count < yearPeak * 0.7;
      cells.push({ year, month: mo, count, medianUnitPriceWan, isBuyerAdvantaged });
    }
  }

  return { district: label, isGlobal, cells, years, sufficient: true };
}

/**
 * Returns the last 12 months of cells from a HeatmapData result,
 * for mobile single-row collapse. The array always has exactly 12 entries.
 */
export function last12Cells(data: HeatmapData): HeatmapCell[] {
  if (!data.sufficient || data.cells.length === 0) return [];
  // Find max year/month in cells
  let maxYear = 0, maxMonth = 0;
  for (const c of data.cells) {
    if (c.year > maxYear || (c.year === maxYear && c.month > maxMonth)) {
      maxYear = c.year;
      maxMonth = c.month;
    }
  }
  // Build list of 12 prior (year, month) pairs
  const pairs: { year: number; month: number }[] = [];
  let y = maxYear, mo = maxMonth;
  for (let i = 0; i < 12; i++) {
    pairs.unshift({ year: y, month: mo });
    mo -= 1;
    if (mo < 1) { mo = 12; y -= 1; }
  }
  return pairs.map(({ year, month }) => {
    return (
      data.cells.find((c) => c.year === year && c.month === month) ?? {
        year, month, count: 0, medianUnitPriceWan: null, isBuyerAdvantaged: false,
      }
    );
  });
}
