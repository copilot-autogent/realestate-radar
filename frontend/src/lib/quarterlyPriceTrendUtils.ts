/**
 * Pure utility functions for quarterly median price sparkline (issue #159).
 * Computes district (or all-Taiwan fallback) median 單價 per quarter over
 * the trailing 6–8 quarters, and a YoY % badge signal.
 *
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export type TrendColor = "green" | "red" | "grey";

export interface QuarterlyPricePoint {
  /** "YYYY-QN" label, e.g. "2024-Q3" */
  label: string;
  /** Median unit price (萬/坪, NT$10k per ping) */
  medianPerPing: number;
  /** Number of qualifying transactions in the quarter */
  count: number;
}

export interface QuarterlyPriceSeries {
  /** District name passed in, or empty string for all-Taiwan */
  district: string;
  /** Ordered ascending from oldest to most-recent quarter */
  points: QuarterlyPricePoint[];
  /**
   * YoY % change: current quarter vs same quarter one year earlier.
   * Null when the same quarter from last year is unavailable.
   */
  yoyPct: number | null;
  /**
   * Visual signal based on yoyPct:
   *  "green"  → yoyPct ≤ -2%  (falling prices = buyer advantage)
   *  "red"    → yoyPct ≥ +2%  (rising prices = caution)
   *  "grey"   → within ±2% or null
   */
  trendColor: TrendColor;
  /**
   * Human-readable badge string, e.g. "+3.2% YoY ↑" or "-5.1% YoY ↓".
   * Empty string when yoyPct is null.
   */
  yoyBadge: string;
  /**
   * true when ≥ 4 quarterly data points are present.
   * Corresponds to the minimum required for a meaningful sparkline.
   */
  sufficient: boolean;
  /**
   * true when this series was computed using all-Taiwan data because the
   * selected district had insufficient transactions.
   */
  isFallback: boolean;
}

// ── Constants ─────────────────────────────────────────────────────────────────

/** Minimum transactions per quarter for a district bucket to be included */
const MIN_QUARTER_TX = 10;

/** Minimum distinct quarters needed for the series to be "sufficient" */
const MIN_QUARTERS = 4;

/** Minimum total transactions needed to include a fallback (all-Taiwan) bucket */
const MIN_FALLBACK_TX = 10;

/** How many trailing quarters to show in the sparkline (6–8) */
const MAX_QUARTERS = 8;

// ── Internal helpers ──────────────────────────────────────────────────────────

/** Median of an unsorted array. Returns null for empty input. */
function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
}

/**
 * Parse year and quarter from a raw date string.
 * Accepts YYYY-MM-DD, YYYY/MM/DD, and YYYYMMDD formats.
 * Normalises with NFKC before regex to handle full-width CJK digits (U+FF10–FF19).
 * Returns { year, quarter: 1–4 } or null on failure.
 */
export function parseDateQuarter(raw: unknown): { year: number; quarter: 1 | 2 | 3 | 4 } | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");

  let year: number, month: number;

  // YYYYMMDD — must match exactly 8 digits (anchored) to avoid matching YYYY-MM-DD via this branch
  const compact = /^(\d{4})(\d{2})\d{2}$/.exec(s);
  if (compact) {
    year  = parseInt(compact[1]!, 10);
    month = parseInt(compact[2]!, 10);
  } else {
    // YYYY-MM-DD or YYYY/MM/DD (separator required after month)
    const sep = /^(\d{4})[-/](\d{1,2})(?:[-/]\d|$)/.exec(s);
    if (!sep) return null;
    year  = parseInt(sep[1]!, 10);
    month = parseInt(sep[2]!, 10);
  }

  if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
    return null;
  }

  const quarter = Math.ceil(month / 3) as 1 | 2 | 3 | 4;
  return { year, quarter };
}

/** Build a "YYYY-QN" label from year and quarter. */
export function quarterLabel(year: number, quarter: 1 | 2 | 3 | 4): string {
  return `${year}-Q${quarter}`;
}

/**
 * Compute YoY % change: (recent − prior) / prior × 100.
 * Returns null when either value is non-finite or prior is 0.
 */
export function computeQuarterlyYoY(recent: number, prior: number): number | null {
  if (!isFinite(recent) || !isFinite(prior) || prior === 0) return null;
  return ((recent - prior) / prior) * 100;
}

/** Derive trend color from a YoY percentage value. */
export function yoyToTrendColor(yoyPct: number | null): TrendColor {
  if (yoyPct === null) return "grey";
  if (yoyPct <= -2) return "green";
  if (yoyPct >= 2)  return "red";
  return "grey";
}

/** Format a YoY percentage as a human-readable badge string. */
export function formatYoYBadge(yoyPct: number | null): string {
  if (yoyPct === null) return "";
  if (yoyPct > 0) return `+${yoyPct.toFixed(1)}% YoY ↑`;
  if (yoyPct < 0) return `${yoyPct.toFixed(1)}% YoY ↓`;
  return `0.0% YoY →`;
}

// ── Core computation ──────────────────────────────────────────────────────────

/**
 * Accumulate quarterly median 單價 buckets from a GeoJSON feature array.
 *
 * @param features     GeoJSON feature array (rawAllFeatures from Map.astro)
 * @param districtId   District to filter on; empty/null for all-Taiwan aggregation
 * @param city         Optional city filter (applied only when districtId is non-empty)
 * @returns Map keyed by "YYYY-QN" → array of unit prices (萬/坪) from qualifying transactions
 */
function accumulateQuarterBuckets(
  features: any[],
  districtId: string,
  city?: string,
): Map<string, number[]> {
  const normDistrict = districtId ? districtId.normalize("NFKC") : "";
  const normCity     = city?.normalize("NFKC");

  const buckets = new Map<string, number[]>();

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;

    // District filter
    if (normDistrict) {
      if (typeof p.district !== "string") continue;
      if (p.district.normalize("NFKC") !== normDistrict) continue;

      if (normCity) {
        if (typeof p.city !== "string" || p.city.normalize("NFKC") !== normCity) continue;
      }
    }

    const up = Number(p.unitPrice);
    if (!isFinite(up) || up <= 0) continue;

    const parsed = parseDateQuarter(p.date);
    if (!parsed) continue;

    const key = quarterLabel(parsed.year, parsed.quarter);
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(up);
  }

  return buckets;
}

/**
 * Build a QuarterlyPriceSeries for a district (or all-Taiwan fallback).
 *
 * Algorithm:
 * 1. Compute district buckets; if the most-recent quarter or total count is
 *    insufficient, fall back to all-Taiwan buckets.
 * 2. Keep at most MAX_QUARTERS trailing quarters.
 * 3. Compute per-quarter median 單價 (convert from NT$/坪 → 萬/坪).
 * 4. Compute YoY between the most-recent quarter and the same quarter one year earlier.
 *
 * @param features    GeoJSON feature array (rawAllFeatures from Map.astro)
 * @param districtId  District name to filter on; empty/null triggers all-Taiwan mode
 * @param city        Optional city filter
 * @returns QuarterlyPriceSeries
 */
export function computeQuarterlyPriceSeries(
  features: any[],
  districtId: string,
  city?: string,
): QuarterlyPriceSeries {
  const EMPTY: QuarterlyPriceSeries = {
    district: districtId,
    points: [],
    yoyPct: null,
    trendColor: "grey",
    yoyBadge: "",
    sufficient: false,
    isFallback: false,
  };

  if (!features.length) return EMPTY;

  /**
   * Build qualified points from a bucket map.
   * Filters quarters below minTx FIRST, then takes the MAX_QUARTERS most-recent
   * qualifying quarters (avoids sparse recent quarters consuming the window).
   */
  function buildPoints(
    buckets: Map<string, number[]>,
    minTx: number,
  ): QuarterlyPricePoint[] {
    // Filter to quarters with enough transactions, sort descending (most recent first)
    const qualifyingKeys = [...buckets.keys()]
      .filter((key) => (buckets.get(key)?.length ?? 0) >= minTx)
      .sort()
      .reverse();

    // Take at most MAX_QUARTERS qualifying quarters, then sort ascending for display
    const windowKeys = qualifyingKeys.slice(0, MAX_QUARTERS).sort();

    return windowKeys.flatMap((key) => {
      const prices = buckets.get(key)!;
      const med = median(prices);
      if (med === null) return [];
      return [{ label: key, medianPerPing: med / 10000, count: prices.length }];
    });
  }

  let isFallback = false;
  let points: QuarterlyPricePoint[] = [];

  if (districtId) {
    const districtBuckets = accumulateQuarterBuckets(features, districtId, city);
    const districtPoints  = buildPoints(districtBuckets, MIN_QUARTER_TX);

    // Fall back when the district yields fewer than MIN_QUARTERS qualifying quarters
    if (districtPoints.length < MIN_QUARTERS) {
      isFallback = true;
      const fallbackBuckets = accumulateQuarterBuckets(features, "");
      points = buildPoints(fallbackBuckets, MIN_FALLBACK_TX);
    } else {
      points = districtPoints;
    }
  } else {
    // No district selected — use all-Taiwan from the start
    isFallback = true;
    const fallbackBuckets = accumulateQuarterBuckets(features, "");
    points = buildPoints(fallbackBuckets, MIN_FALLBACK_TX);
  }

  if (points.length === 0) return { ...EMPTY, isFallback };

  const sufficient = points.length >= MIN_QUARTERS;

  // YoY: compare most-recent quarter to the same quarter one year earlier
  const lastPoint  = points[points.length - 1]!;
  const [lastYear, lastQ] = lastPoint.label.split("-Q");
  const priorLabel = `${Number(lastYear) - 1}-Q${lastQ}`;
  const priorPoint = points.find((p) => p.label === priorLabel) ?? null;

  const yoyPct = priorPoint
    ? computeQuarterlyYoY(lastPoint.medianPerPing, priorPoint.medianPerPing)
    : null;

  const trendColor = yoyToTrendColor(yoyPct);
  const yoyBadge   = formatYoYBadge(yoyPct);

  return {
    district: districtId,
    points,
    yoyPct,
    trendColor,
    yoyBadge,
    sufficient,
    isFallback,
  };
}
