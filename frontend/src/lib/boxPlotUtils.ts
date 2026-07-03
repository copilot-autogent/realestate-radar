/**
 * Pure utility functions for the per-district price-per-ping box-plot chart (issue #124).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export interface BoxPlotStats {
  yearMonth: string;  // label, e.g. "2024"
  min: number;
  q1: number;
  median: number;
  q3: number;
  max: number;
  count: number;
}

export interface BoxPlotData {
  district: string;
  city?: string;
  plots: BoxPlotStats[];         // one entry per calendar year (up to maxYears)
  percentileRank: number | null; // 0–100, or null when insufficient history
  currentMedian: number | null;  // most-recent year's median (NT$/坪)
  sufficient: boolean;           // false when < 2 years of data
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function sortedNums(arr: number[]): number[] {
  return [...arr].sort((a, b) => a - b);
}

/**
 * Inclusive quartile interpolation (equivalent to Excel QUARTILE.INC / numpy default).
 * Assumes `sorted` is already sorted ascending and non-empty.
 */
function quartile(sorted: number[], p: number): number {
  if (sorted.length === 1) return sorted[0]!;
  const idx = p * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (idx - lo) * (sorted[hi]! - sorted[lo]!);
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Parse the calendar year from a date string (YYYY-MM-DD or YYYY/MM/DD).
 * Normalises full-width digits with NFKC before parsing.
 * Returns null on any parse failure.
 */
export function parseDateYear(raw: unknown): number | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  const m = /^(\d{4})[-/]/.exec(s);
  if (!m) return null;
  const year = parseInt(m[1]!, 10);
  return isNaN(year) || year < 1900 || year > 2100 ? null : year;
}

/**
 * Compute box-plot statistics (5-number summary) for a set of transactions.
 *
 * @param txArray    Array of objects with a numeric `unitPrice` field (NT$/坪)
 * @param yearLabel  Label for this group (e.g. "2024")
 * @returns BoxPlotStats, or null when txArray has no valid positive prices
 */
export function computeBoxPlot(
  txArray: Array<{ unitPrice?: unknown }>,
  yearLabel: string
): BoxPlotStats | null {
  const prices = txArray
    .map((tx) => {
      const v = Number(tx.unitPrice);
      return isFinite(v) && v > 0 ? v : null;
    })
    .filter((v): v is number => v !== null);

  if (prices.length === 0) return null;

  const sorted = sortedNums(prices);
  return {
    yearMonth: yearLabel,
    min:    sorted[0]!,
    q1:     quartile(sorted, 0.25),
    median: quartile(sorted, 0.5),
    q3:     quartile(sorted, 0.75),
    max:    sorted[sorted.length - 1]!,
    count:  sorted.length,
  };
}

/**
 * Compute the percentile rank (0–100) of `currentMedian` within a pool of
 * historical box-plot medians using linear interpolation (PERCENTRANK.INC).
 *
 * Returns 50 when `historicalBoxPlots` is empty (neutral fallback).
 */
export function computePercentileRank(
  currentMedian: number,
  historicalBoxPlots: BoxPlotStats[]
): number {
  if (historicalBoxPlots.length === 0) return 50;

  const medians = sortedNums(historicalBoxPlots.map((bp) => bp.median));

  if (medians.length === 1) {
    return currentMedian >= medians[0]! ? 100 : 0;
  }

  if (currentMedian <= medians[0]!) return 0;
  if (currentMedian >= medians[medians.length - 1]!) return 100;

  for (let i = 0; i < medians.length - 1; i++) {
    const lo = medians[i]!;
    const hi = medians[i + 1]!;
    if (currentMedian >= lo && currentMedian <= hi) {
      const rank = (i + (currentMedian - lo) / (hi - lo)) / (medians.length - 1);
      return Math.round(rank * 100);
    }
  }

  return 50; // unreachable guard
}

/**
 * Group raw GeoJSON features by calendar year for a district, compute box-plot
 * statistics per year, and derive a percentile rank for the most recent year.
 *
 * @param features  GeoJSON feature array (rawAllFeatures from Map.astro)
 * @param district  District name to filter
 * @param city      Optional city filter
 * @param maxYears  Trailing calendar years to include (default 3)
 */
export function computeDistrictBoxPlots(
  features: any[],
  district: string,
  city?: string,
  maxYears = 3
): BoxPlotData {
  const EMPTY: BoxPlotData = {
    district,
    city,
    plots: [],
    percentileRank: null,
    currentMedian: null,
    sufficient: false,
  };

  if (!features.length) return EMPTY;

  // Determine anchor year from the latest transaction date in this district
  let latestYear = 0;
  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;
    if (p.district !== district) continue;
    if (city && p.city && p.city !== city) continue;
    const yr = parseDateYear(p.date);
    if (yr && yr > latestYear) latestYear = yr;
  }
  if (latestYear === 0) return EMPTY;

  // Collect unit prices per year within the rolling window
  const yearPrices: Record<number, number[]> = {};
  for (let y = latestYear - maxYears + 1; y <= latestYear; y++) {
    yearPrices[y] = [];
  }

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;
    if (p.district !== district) continue;
    if (city && p.city && p.city !== city) continue;
    const yr = parseDateYear(p.date);
    if (!yr || !(yr in yearPrices)) continue;
    const up = Number(p.unitPrice);
    if (!isFinite(up) || up <= 0) continue;
    yearPrices[yr].push(up);
  }

  // Compute box-plots for each year that has data
  const plots: BoxPlotStats[] = [];
  for (const yr of Object.keys(yearPrices).map(Number).sort()) {
    const bp = computeBoxPlot(
      yearPrices[yr]!.map((v) => ({ unitPrice: v })),
      String(yr)
    );
    if (bp) plots.push(bp);
  }

  if (plots.length < 2) {
    return { ...EMPTY, plots, sufficient: false };
  }

  // Percentile rank of the most recent year's median vs all prior years
  const latestPlot = plots[plots.length - 1]!;
  const historicalPlots = plots.slice(0, -1);
  const percentileRank = computePercentileRank(latestPlot.median, historicalPlots);

  return {
    district,
    city,
    plots,
    percentileRank,
    currentMedian: latestPlot.median,
    sufficient: true,
  };
}
