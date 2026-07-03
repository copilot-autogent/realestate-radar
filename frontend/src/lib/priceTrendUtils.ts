/**
 * Pure utility functions for monthly price-per-ping trend charts (issue #131).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export interface TrendPoint {
  /** "YYYY-MM" — always ASCII digits after NFKC normalisation */
  month: string;
  /** Median unit price (NT$/坪) for the month */
  medianPerPing: number;
  /**
   * Median total transaction price in 萬 NTD (10,000 NTD units).
   * Null when no valid totalPrice data is available for this month.
   */
  medianTotalWan: number | null;
  /** Number of qualifying transactions */
  count: number;
}

export interface PriceTrendSeries {
  district: string;
  city?: string;
  /** Sorted ascending by month, trimmed to ≤ maxMonths trailing months */
  points: TrendPoint[];
  /**
   * 3-month moving average of medianPerPing aligned to points[].
   * Null for the first two entries (insufficient history).
   */
  ma3: (number | null)[];
  /**
   * 3-month moving average of medianTotalWan aligned to points[].
   * Null for the first two entries or months lacking total price data.
   */
  ma3Total: (number | null)[];
  /** YoY % change of the most recent month's medianPerPing vs same month one year earlier. Null when unavailable. */
  yoyPercentage: number | null;
  /** YoY % change of the most recent month's medianTotalWan vs same month one year earlier. Null when unavailable. */
  yoyTotalPercentage: number | null;
  /** false when fewer than 6 monthly data points exist for the district */
  sufficient: boolean;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/** Sort array ascending; returns a new array. */
function sortedNums(arr: number[]): number[] {
  return [...arr].sort((a, b) => a - b);
}

/** Median of a sorted ascending array. Returns 0 for empty input. */
function median(sorted: number[]): number {
  if (sorted.length === 0) return 0;
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Parse a "YYYY-MM" transaction month from a raw date string.
 * Accepts YYYY-MM-DD, YYYY/MM/DD, YYYY-M-DD, and YYYY-MM formats.
 * Normalises with NFKC before regex to handle full-width CJK digits.
 * Returns null on any parse failure or out-of-range values.
 */
export function parseDateMonth(raw: unknown): string | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  // Accept 1 or 2 digit months (e.g. "2024-3-15" as well as "2024-03-15")
  const m = /^(\d{4})[-/](\d{1,2})/.exec(s);
  if (!m) return null;
  const year  = parseInt(m[1]!, 10);
  const month = parseInt(m[2]!, 10);
  if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
    return null;
  }
  return `${m[1]!}-${String(month).padStart(2, "0")}`;
}

/**
 * Compute the minimum "YYYY-MM" string that is exactly `months` months before `anchorMonth`.
 * Used to build the 36-month trailing window.
 */
export function subtractMonths(anchorMonth: string, months: number): string {
  const [ys, ms] = anchorMonth.split("-");
  let year  = parseInt(ys!, 10);
  let month = parseInt(ms!, 10);
  month -= months;
  while (month <= 0) {
    month += 12;
    year  -= 1;
  }
  return `${year}-${String(month).padStart(2, "0")}`;
}

/**
 * Compute 3-month moving average of `values`.
 * Returns an array the same length as `values`.
 * The first two entries are null (insufficient lookback).
 */
export function computeMA3(values: number[]): (number | null)[] {
  return values.map((_, i) => {
    if (i < 2) return null;
    const slice = [values[i - 2]!, values[i - 1]!, values[i]!];
    return (slice[0] + slice[1] + slice[2]) / 3;
  });
}

/**
 * Compute YoY % change: (recent − prior) / prior × 100.
 * Returns null when either value is non-finite, or prior is 0.
 */
export function computeYoY(recent: number, prior: number): number | null {
  if (!isFinite(recent) || !isFinite(prior) || prior === 0) return null;
  return ((recent - prior) / prior) * 100;
}

/**
 * Core computation: build a monthly median price-per-ping series for a district.
 *
 * @param features   GeoJSON feature array (rawAllFeatures from Map.astro)
 * @param districtId District name to filter on (e.g. "大安區")
 * @param city       Optional city filter (e.g. "台北市")
 * @param maxMonths  Trailing-month window size (default 36)
 * @returns PriceTrendSeries — sufficient = false when < 6 monthly data points
 */
export function computePriceTrendSeries(
  features: any[],
  districtId: string,
  city?: string,
  maxMonths = 36,
): PriceTrendSeries {
  const EMPTY: PriceTrendSeries = {
    district: districtId,
    city,
    points: [],
    ma3: [],
    ma3Total: [],
    yoyPercentage: null,
    yoyTotalPercentage: null,
    sufficient: false,
  };

  if (!features.length || !districtId) return EMPTY;

  const normDistrict = districtId.normalize("NFKC");
  const normCity     = city?.normalize("NFKC");

  // Accumulate per-ping and total prices keyed by "YYYY-MM"
  const monthPerPing  = new Map<string, number[]>();
  const monthTotalWan = new Map<string, number[]>();
  let latestMonth = "";

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;

    if (typeof p.district !== "string") continue;
    if (p.district.normalize("NFKC") !== normDistrict) continue;

    if (normCity) {
      if (typeof p.city !== "string" || p.city.normalize("NFKC") !== normCity) continue;
    }

    const up = Number(p.unitPrice);
    if (!isFinite(up) || up <= 0) continue;

    const monthKey = parseDateMonth(p.date);
    if (!monthKey) continue;

    if (monthKey > latestMonth) latestMonth = monthKey;

    if (!monthPerPing.has(monthKey))  monthPerPing.set(monthKey, []);
    if (!monthTotalWan.has(monthKey)) monthTotalWan.set(monthKey, []);

    monthPerPing.get(monthKey)!.push(up);

    const tp = Number(p.totalPrice);
    if (isFinite(tp) && tp > 0) {
      if (!monthTotalWan.has(monthKey)) monthTotalWan.set(monthKey, []);
      monthTotalWan.get(monthKey)!.push(tp / 10000);
    }
  }

  if (!latestMonth) return EMPTY;

  // Build the trailing window
  const windowStart = subtractMonths(latestMonth, maxMonths - 1);

  const points: TrendPoint[] = [];
  for (const [month, perPings] of monthPerPing) {
    if (month < windowStart || month > latestMonth) continue;
    const sortedPP  = sortedNums(perPings);
    const totals    = monthTotalWan.get(month);
    // medianTotalWan is null when no valid totalPrice data exists for this month
    const medianTotalWan = totals && totals.length > 0 ? median(sortedNums(totals)) : null;
    points.push({
      month,
      medianPerPing:  median(sortedPP),
      medianTotalWan,
      count: sortedPP.length,
    });
  }

  // Sort chronologically
  points.sort((a, b) => (a.month < b.month ? -1 : a.month > b.month ? 1 : 0));

  if (points.length < 6) {
    return { ...EMPTY, points, sufficient: false };
  }

  // MA3 for per-ping (via shared helper)
  const ma3 = computeMA3(points.map((pt) => pt.medianPerPing));

  // MA3 for total price — null propagated when month lacks total price data
  const ma3Total: (number | null)[] = points.map((_, i) => {
    if (i < 2) return null;
    const v0 = points[i - 2]!.medianTotalWan;
    const v1 = points[i - 1]!.medianTotalWan;
    const v2 = points[i]!.medianTotalWan;
    if (v0 === null || v1 === null || v2 === null) return null;
    return (v0 + v1 + v2) / 3;
  });

  // YoY: compare last point to same month one year earlier
  const lastPoint = points[points.length - 1]!;
  const priorMonthKey = subtractMonths(lastPoint.month, 12);
  const priorPoint = points.find((pt) => pt.month === priorMonthKey) ?? null;
  const yoyPercentage = priorPoint
    ? computeYoY(lastPoint.medianPerPing, priorPoint.medianPerPing)
    : null;
  const yoyTotalPercentage =
    priorPoint && lastPoint.medianTotalWan !== null && priorPoint.medianTotalWan !== null
      ? computeYoY(lastPoint.medianTotalWan, priorPoint.medianTotalWan)
      : null;

  return {
    district: districtId,
    city,
    points,
    ma3,
    ma3Total,
    yoyPercentage,
    yoyTotalPercentage,
    sufficient: true,
  };
}
