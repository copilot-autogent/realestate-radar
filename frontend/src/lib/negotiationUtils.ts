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
