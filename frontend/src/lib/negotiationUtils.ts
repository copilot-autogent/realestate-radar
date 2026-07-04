/**
 * Pure utility functions for the per-district negotiation margin estimator (issue #122).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Formula:
 *   raw_margin = BASE_MARGIN × velocityMultiplier × peakFactor × assessedRatioFactor
 *   margin = clamp(raw_margin, 0, MAX_MARGIN)
 *   output range = margin ± SPREAD_PCT of margin
 */

/** Minimum transaction count required for a reliable estimate. */
export const MIN_TX = 20;

/** Base negotiation margin for a flat/neutral market. */
const BASE_MARGIN = 0.05; // 5%

/** Hard cap on the margin estimate to prevent misleading outliers. */
export const MAX_MARGIN = 0.20; // 20%

/** Half-width of the low/high confidence band around the centre estimate. */
const SPREAD_FACTOR = 0.20; // ±20% of margin

// ── Input / output types ──────────────────────────────────────────────────────

/** Pre-computed per-district statistics passed into the estimator. */
export interface DistrictStats {
  /** Transaction count in the last 12 months. */
  txCount12mo: number;
  /** YoY transaction volume Δ% (null when prior-period data is absent). */
  velocityYoYPct: number | null;
  /** Months elapsed since the district's median-price quarterly peak (null when data is sparse). */
  monthsSincePeak: number | null;
  /** Assessed-to-market value ratio 公告現值比 (null when not provided in data). */
  assessedToMarketRatio: number | null;
  /** Median unit transaction price in the last 12 months, in 萬/坪 (null when no data). */
  medianUnitPriceWan: number | null;
}

/** The estimated negotiation margin for a district. */
export interface NegotiationEstimate {
  /** Whether sufficient data is available (txCount12mo ≥ MIN_TX). */
  sufficient: boolean;
  /** "資料充足" when txCount12mo ≥ 20; "資料有限" when 10–19; "資料不足" when < 10. */
  confidenceLabel: "資料充足" | "資料有限" | "資料不足";
  /** Low-end negotiation margin % (0–1 scale). Null when insufficient. */
  marginLow: number | null;
  /** Central negotiation margin % (0–1 scale). Null when insufficient. */
  marginCenter: number | null;
  /** High-end negotiation margin % (0–1 scale). Null when insufficient. */
  marginHigh: number | null;
  /** Low-end NT$ negotiation headroom per 坪, in 萬. Null when median price unavailable. */
  rangeLowWan: number | null;
  /** High-end NT$ negotiation headroom per 坪, in 萬. Null when median price unavailable. */
  rangeHighWan: number | null;
}

// ── Internal component multipliers ───────────────────────────────────────────

/**
 * Velocity multiplier: falling transaction volume → more negotiation room.
 * YoY Δ% tiers (inclusive upper bound):
 *   ≤ −20 % → 1.5 (large supply excess)
 *   −20 < x ≤ −10 % → 1.3
 *   −10 < x < 0 % → 1.1
 *   0 ≤ x < +10 % → 1.0 (neutral)
 *   +10 ≤ x < +20 % → 0.85
 *   ≥ +20 % → 0.70 (hot market, little room)
 */
export function velocityMultiplier(yoyPct: number | null): number {
  if (yoyPct === null) return 1.0;
  if (yoyPct <= -20) return 1.5;
  if (yoyPct <= -10) return 1.3;
  if (yoyPct <   0) return 1.1;
  if (yoyPct <  10) return 1.0;
  if (yoyPct <  20) return 0.85;
  return 0.70;
}

/**
 * Peak-time factor: longer since price peak → more negotiation room.
 *   > 24 months since peak → 1.3
 *   > 12 to 24 months → 1.15
 *   ≥ 6 to 12 months → 1.0 (neutral)
 *   < 6 months → 0.85 (still near peak)
 */
export function peakFactor(monthsSincePeak: number | null): number {
  if (monthsSincePeak === null) return 1.0;
  if (monthsSincePeak >  24) return 1.3;
  if (monthsSincePeak >  12) return 1.15;
  if (monthsSincePeak >=  6) return 1.0;
  return 0.85;
}

/**
 * Assessed-to-market ratio factor (公告現值比).
 * A low ratio means market price greatly exceeds assessed value (speculative premium) →
 * buyers have more room to negotiate below the "inflated" ask price.
 *   < 0.2  → 1.30
 *   0.2 to < 0.4 → 1.15
 *   0.4 to ≤ 0.6 → 1.0 (neutral)
 *   > 0.6  → 0.90 (market price closer to assessed → less speculation buffer)
 * Null (unavailable) defaults to 1.0 (no adjustment).
 */
export function assessedRatioFactor(ratio: number | null): number {
  if (ratio === null) return 1.0;
  if (ratio <  0.2) return 1.3;
  if (ratio <  0.4) return 1.15;
  if (ratio <= 0.6) return 1.0;
  return 0.9;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Estimates the per-district negotiation margin from pre-computed district stats.
 *
 * Returns `sufficient: false` when txCount12mo < MIN_TX (20).
 * Margin is capped at MAX_MARGIN (20%) to prevent misleading signals on extreme outliers.
 */
export function estimateNegotiationMargin(stats: DistrictStats): NegotiationEstimate {
  // Confidence label
  let confidenceLabel: NegotiationEstimate["confidenceLabel"];
  if (stats.txCount12mo >= MIN_TX) {
    confidenceLabel = "資料充足";
  } else if (stats.txCount12mo >= 10) {
    confidenceLabel = "資料有限";
  } else {
    confidenceLabel = "資料不足";
  }

  if (stats.txCount12mo < MIN_TX) {
    return {
      sufficient: false,
      confidenceLabel,
      marginLow: null,
      marginCenter: null,
      marginHigh: null,
      rangeLowWan: null,
      rangeHighWan: null,
    };
  }

  const raw =
    BASE_MARGIN *
    velocityMultiplier(stats.velocityYoYPct) *
    peakFactor(stats.monthsSincePeak) *
    assessedRatioFactor(stats.assessedToMarketRatio);

  const center = Math.min(raw, MAX_MARGIN);
  const low    = Math.min(Math.max(center * (1 - SPREAD_FACTOR), 0.01), center);
  const high   = Math.min(center * (1 + SPREAD_FACTOR), MAX_MARGIN);

  const medianWan = stats.medianUnitPriceWan;
  const rangeLowWan  = medianWan !== null ? parseFloat((medianWan * low).toFixed(2))  : null;
  const rangeHighWan = medianWan !== null ? parseFloat((medianWan * high).toFixed(2)) : null;

  return {
    sufficient: true,
    confidenceLabel,
    marginLow: parseFloat(low.toFixed(4)),
    marginCenter: parseFloat(center.toFixed(4)),
    marginHigh: parseFloat(high.toFixed(4)),
    rangeLowWan,
    rangeHighWan,
  };
}

/**
 * Computes the percentile rank (0–100) of a district's negotiation margin
 * relative to a population of other districts' margin estimates.
 *
 * Useful for "This district is at the Nth percentile vs. past 12 months" context.
 * Returns null when `target` is insufficient or `others` contains no sufficient estimates.
 * The `others` pool may or may not include `target`; it doesn't affect the ranking.
 *
 * @param target  The district estimate to rank.
 * @param others  Pool of district estimates to rank against.
 */
export function computeNegotiationPercentile(
  target: NegotiationEstimate,
  others: NegotiationEstimate[]
): number | null {
  if (!target.sufficient || target.marginCenter === null) return null;
  const margins = others
    .filter((e) => e.sufficient && e.marginCenter !== null)
    .map((e) => e.marginCenter as number);
  if (margins.length < 1) return null;
  const below = margins.filter((m) => m < (target.marginCenter as number)).length;
  return Math.round((below / margins.length) * 100);
}

// ── Trend chart types & computation ────────────────────────────────────

/**
 * A single monthly data point for the negotiation margin trend chart.
 * Represents the estimated negotiation margin for a given month.
 */
export interface MarginDataPoint {
  /** "YYYY-MM" — always ASCII digits (NFKC-normalised) */
  month: string;
  /** Central negotiation margin estimate (0–1 scale) for this month */
  marginCenter: number;
  /** Low-end negotiation margin estimate (0–1 scale) */
  marginLow: number;
  /** High-end negotiation margin estimate (0–1 scale) */
  marginHigh: number;
}

/** Minimum number of transactions required within a target month itself to compute a trend data point. */
export const MIN_TREND_TX = 5;

/** Minimum number of computable months required to show the trend chart (vs. "資料不足"). */
export const MIN_TREND_MONTHS = 3;

// ── Internal helpers ──────────────────────────────────────────────────────────

/**
 * Parse a month label "YYYY-MM" from a Date, normalising with NFKC first.
 * Returns null on invalid input.
 */
function dateToMonthLabel(d: Date): string | null {
  if (!(d instanceof Date) || isNaN(d.getTime())) return null;
  const year  = d.getUTCFullYear();
  const month = d.getUTCMonth() + 1;
  return `${year}-${String(month).padStart(2, "0")}`;
}

/**
 * Parse a raw date value (string or Date) and return a "YYYY-MM" label.
 * Handles full-width CJK digits via NFKC normalisation.
 * Returns null on failure.
 */
function parseToMonthLabel(raw: unknown): string | null {
  if (raw instanceof Date) return dateToMonthLabel(raw);
  if (typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  const m =
    /^(\d{4})[-/](\d{1,2})(?:[-/]\d{1,2})?(?:[T\s]|$)/.exec(s) ??
    /^(\d{4})(\d{2})\d{2}$/.exec(s);
  if (!m) return null;
  const year  = parseInt(m[1]!, 10);
  const month = parseInt(m[2]!, 10);
  if (year < 1900 || year > 2100 || month < 1 || month > 12) return null;
  return `${year}-${String(month).padStart(2, "0")}`;
}

/** Add `delta` months to a "YYYY-MM" label. Returns a new "YYYY-MM" string. */
function addMonths(label: string, delta: number): string {
  const year  = parseInt(label.slice(0, 4), 10);
  const month = parseInt(label.slice(5, 7), 10);
  const total = (year * 12 + (month - 1)) + delta;
  const y = Math.floor(total / 12);
  const m = (total % 12) + 1;
  return `${y}-${String(m).padStart(2, "0")}`;
}

/** Compare two "YYYY-MM" labels. Returns negative if a < b, 0 if equal, positive if a > b. */
function cmpMonth(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Sorted median of a numeric array. Returns null for empty input. */
function medianOf(nums: number[]): number | null {
  if (nums.length === 0) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[mid]!
    : (sorted[mid - 1]! + sorted[mid]!) / 2;
}

// ── Public API ──────────────────────────────────────

/**
 * Compute a time-series of monthly negotiation margin estimates for a district.
 *
 * For each of the past `months` calendar months (ending at `referenceMonth`, or the
 * most recent month in `transactions` when omitted), this function:
 *   1. Slices the transaction array to the 12-month window ending at the target month.
 *   2. Derives `DistrictStats` from that slice (velocity, peak recency, assessed ratio, median price).
 *   3. Runs `estimateNegotiationMargin` to get the margin estimate.
 *   4. Includes a `MarginDataPoint` only when the target month itself has ≥ `MIN_TREND_TX=5`
 *      transactions AND the 12-month window has ≥ `MIN_TX=20` (estimator's `sufficient` guard).
 *
 * Returns a **sparse** array of `(MarginDataPoint | undefined)[]` of length `months`.
 * Index 0 = oldest month; last index = most recent. `undefined` entries are data gaps.
 *
 * NFKC normalisation is applied to district names and raw date strings to handle
 * full-width CJK digits (U+FF10–FF19) common in Taiwan gov open data.
 *
 * @param transactions   All Transaction objects for the dataset (any district/city)
 * @param districtId     Target district name (e.g. "大安區"); NFKC comparison applied
 * @param months         Number of trailing months to compute (e.g. 12)
 * @param referenceMonth Optional anchor "YYYY-MM"; defaults to most recent tx month
 * @returns              Sparse series of length `months`; undefined = insufficient data
 */
export function computeNegotiationMarginTrend(
  transactions: Array<{ district: string; transactionDate: Date; unitPrice?: number | null; assessedToMarketRatio?: number | null; [key: string]: unknown }>,
  districtId: string,
  months: number,
  referenceMonth?: string,
): (MarginDataPoint | undefined)[] {
  const normDistrict = districtId.normalize("NFKC");

  // ── Step 1: Filter to district, parse labels month ─────────────────
  interface ParsedTx {
    month: string;
    unitPriceWanPerPing: number | null;
    assessedToMarketRatio: number | null;
  }

  const districtTx: ParsedTx[] = [];
  for (const tx of transactions) {
    if (typeof tx.district !== "string") continue;
    if (tx.district.normalize("NFKC") !== normDistrict) continue;

    // transactionDate is typed as Date; also support string for robustness
    const monthLabel = parseToMonthLabel(tx.transactionDate as unknown);
    if (!monthLabel) continue;

    // unitPrice in 元/平方公尺 → convert to 萬/坪
    // 1 坪 = 3.30579 m²; 1 萬 = 10,000 元
    const unitWanPerPing =
      typeof tx.unitPrice === "number" && tx.unitPrice > 0
        ? parseFloat(((tx.unitPrice * 3.30579) / 10_000).toFixed(4))
        : null;

    districtTx.push({
      month: monthLabel,
      unitPriceWanPerPing: unitWanPerPing,
      assessedToMarketRatio: typeof tx.assessedToMarketRatio === "number"
        ? tx.assessedToMarketRatio
        : null,
    });
  }

  // ── Step 2: Determine reference month ─────────────────────────────────────
  let refMonth: string;
  if (referenceMonth) {
    // NFKC-normalize to handle full-width digit input
    refMonth = referenceMonth.normalize("NFKC");
  } else if (districtTx.length > 0) {
    refMonth = districtTx.reduce(
      (max, tx) => (cmpMonth(tx.month, max) > 0 ? tx.month : max),
      districtTx[0]!.month,
    );
  } else {
    // No data at all — return empty sparse array
    return new Array(months).fill(undefined) as undefined[];
  }

  // ── Step 3: Build the month label sequence (oldest → newest) ─────────────
  const monthLabels: string[] = [];
  for (let i = months - 1; i >= 0; i--) {
    monthLabels.push(addMonths(refMonth, -i));
  }

  // Pre-build a lookup: month → ParsedTx[]
  const txByMonth = new Map<string, ParsedTx[]>();
  for (const tx of districtTx) {
    let arr = txByMonth.get(tx.month);
    if (!arr) { arr = []; txByMonth.set(tx.month, arr); }
    arr.push(tx);
  }

  // ── Step 4: For each target month, derive DistrictStats & compute margin ──
  const result: (MarginDataPoint | undefined)[] = new Array(months).fill(undefined);

  // Pre-compute monthly medians (for monthsSincePeak computation)
  // We need all historical months available in the data, not just the requested window
  const allMonths = [...txByMonth.keys()].sort();
  const monthlyMedianPrice = new Map<string, number>();
  for (const m of allMonths) {
    const prices = txByMonth.get(m)!
      .map((tx) => tx.unitPriceWanPerPing)
      .filter((p): p is number => p !== null);
    const med = medianOf(prices);
    if (med !== null) monthlyMedianPrice.set(m, med);
  }

  for (let idx = 0; idx < months; idx++) {
    const targetMonth = monthLabels[idx]!;
    const windowStart = addMonths(targetMonth, -11); // current 12-month window: [M-11, M]
    const priorEnd    = addMonths(targetMonth, -12); // prior window ends here (inclusive, non-overlapping: M-12 < M-11)
    const priorStart  = addMonths(targetMonth, -23); // prior 12-month window: [M-23, M-12]

    // Collect tx in each window (windows are non-overlapping: current [M-11,M], prior [M-23,M-12])
    const windowTx: ParsedTx[] = [];
    const priorTx:  ParsedTx[] = [];

    for (const [m, txArr] of txByMonth) {
      if (cmpMonth(m, windowStart) >= 0 && cmpMonth(m, targetMonth) <= 0) {
        windowTx.push(...txArr);
      }
      if (cmpMonth(m, priorStart) >= 0 && cmpMonth(m, priorEnd) <= 0) {
        priorTx.push(...txArr);
      }
    }

    const txCount12mo = windowTx.length;

    // Gap check: the target month itself must have at least MIN_TREND_TX transactions.
    // This ensures months with no recent activity produce a chart gap even when
    // older transactions remain in the 12-month window.
    const monthTxCount = txByMonth.get(targetMonth)?.length ?? 0;
    if (monthTxCount < MIN_TREND_TX) continue;

    // Velocity YoY %
    const velocityYoYPct: number | null =
      priorTx.length > 0
        ? ((txCount12mo - priorTx.length) / priorTx.length) * 100
        : null;

    // Assessed-to-market ratio: median of available ratios in window
    const ratios = windowTx
      .map((tx) => tx.assessedToMarketRatio)
      .filter((r): r is number => r !== null);
    const assessedToMarketRatio = medianOf(ratios);

    // Median unit price (萬/坪) in window
    const prices = windowTx
      .map((tx) => tx.unitPriceWanPerPing)
      .filter((p): p is number => p !== null);
    const medianUnitPriceWan = medianOf(prices);

    // monthsSincePeak: look at months ≤ targetMonth with known median prices
    // find the month with the highest median price up to targetMonth
    let peakMonth: string | null = null;
    let peakPrice = -Infinity;
    for (const [m, price] of monthlyMedianPrice) {
      if (cmpMonth(m, targetMonth) <= 0 && price > peakPrice) {
        peakPrice = price;
        peakMonth = m;
      }
    }

    let monthsSincePeak: number | null = null;
    if (peakMonth !== null) {
      // compute month difference: targetMonth - peakMonth
      const ty = parseInt(targetMonth.slice(0, 4), 10);
      const tm = parseInt(targetMonth.slice(5, 7), 10);
      const py = parseInt(peakMonth.slice(0, 4), 10);
      const pm = parseInt(peakMonth.slice(5, 7), 10);
      monthsSincePeak = (ty * 12 + (tm - 1)) - (py * 12 + (pm - 1));
    }

    // Build DistrictStats and compute estimate
    const stats: DistrictStats = {
      txCount12mo,
      velocityYoYPct,
      monthsSincePeak,
      assessedToMarketRatio,
      medianUnitPriceWan,
    };

    const estimate = estimateNegotiationMargin(stats);
    if (!estimate.sufficient) continue; // still sparse — need MIN_TX=20

    result[idx] = {
      month: targetMonth,
      marginCenter: estimate.marginCenter!,
      marginLow:    estimate.marginLow!,
      marginHigh:   estimate.marginHigh!,
    };
  }

  return result;
}

/**
 * Compute a 3-month moving average of `marginCenter` values from a sparse trend series.
 * Returns a parallel array of the same length; undefined/null where MA cannot be computed
 * (fewer than 3 consecutive defined points ending at position i).
 *
 * @param series Sparse trend series from `computeNegotiationMarginTrend`
 */
export function computeTrendMA3(series: (MarginDataPoint | undefined)[]): (number | null)[] {
  return series.map((_, i) => {
    const a = series[i - 2];
    const b = series[i - 1];
    const c = series[i];
    if (!a || !b || !c) return null;
    return (a.marginCenter + b.marginCenter + c.marginCenter) / 3;
  });
}

/**
 * Compute the trend badge for a negotiation margin time series.
 *
 * Compares the most recent 3-month moving average to the one from the prior period
 * (3 months earlier) to determine whether the negotiation window is widening or narrowing.
 *
 * Returns:
 *   "▲ 擴大趨勢" — if the 3-month MA is increasing (window widening)
 *   "▼ 收窄趨勢" — if the 3-month MA is decreasing (window narrowing)
 *   null         — when there is insufficient data to compute a meaningful comparison
 *
 * @param series Sparse trend series from `computeNegotiationMarginTrend`
 */
export function computeTrendBadge(
  series: (MarginDataPoint | undefined)[],
): "▲ 擴大趨勢" | "▼ 收窄趨勢" | null {
  const ma3 = computeTrendMA3(series);

  // Find the most recent non-null MA value
  let latestIdx = -1;
  for (let i = ma3.length - 1; i >= 0; i--) {
    if (ma3[i] !== null) { latestIdx = i; break; }
  }
  if (latestIdx < 0) return null;

  // Find a prior MA value (at least 3 positions earlier) that is non-null
  let priorIdx = -1;
  for (let i = latestIdx - 3; i >= 0; i--) {
    if (ma3[i] !== null) { priorIdx = i; break; }
  }
  if (priorIdx < 0) return null;

  const latest = ma3[latestIdx]!;
  const prior  = ma3[priorIdx]!;

  if (latest > prior) return "▲ 擴大趨勢";
  if (latest < prior) return "▼ 收窄趨勢";
  return null; // flat — no badge
}

/**
 * Determine whether the trend series has sufficient computable months to display.
 *
 * Returns "資料不足" when fewer than `MIN_TREND_MONTHS` (3) defined data points exist.
 * Returns "資料有限" when 3–5 points. Returns "資料充足" when ≥ 6 points.
 */
export function trendSufficiencyLabel(
  series: (MarginDataPoint | undefined)[],
): "資料充足" | "資料有限" | "資料不足" {
  const count = series.filter((p) => p !== undefined).length;
  if (count < MIN_TREND_MONTHS) return "資料不足";
  if (count < 6) return "資料有限";
  return "資料充足";
}
