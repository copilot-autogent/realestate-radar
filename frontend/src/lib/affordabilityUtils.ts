/**
 * Pure utility functions for the 首購族負擔能力地圖 (affordability heat-map) feature (issue #133).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Formula (per issue spec):
 *   maxAffordablePrice = min(downPayment / 0.2, monthlyIncome × 12 × 25)
 *   affordabilityRatio = maxAffordablePrice / districtMedianPrice × 100
 *
 * Tier thresholds:
 *   🟢 affordable  — ratio ≥ 90%
 *   🟡 stretch     — 60% ≤ ratio < 90%
 *   🔴 outOfRange  — ratio < 60%
 */

// ── Types ─────────────────────────────────────────────────────────────────────

export type AffordabilityTier = "affordable" | "stretch" | "outOfRange";

/**
 * Result of a per-district affordability computation.
 * All monetary values are in NT$ (full amount, not 萬).
 */
export interface AffordabilityResult {
  /** Maximum total property price the buyer can afford (NT$). */
  maxAffordable: number;
  /** Affordability ratio as a percentage (0–100+). Values > 100 mean the buyer has surplus. */
  ratio: number;
  /** Tier classification based on the ratio. */
  tier: AffordabilityTier;
  /**
   * Gap = districtMedianPrice − maxAffordable (NT$).
   * Positive → shortfall (尚差 X 萬).
   * Negative → surplus (還有 X 萬空間).
   */
  gap: number;
}

// ── Constants ─────────────────────────────────────────────────────────────────

/** Down-payment assumption: buyer puts 20% down. */
export const DOWN_PAYMENT_RATIO = 0.2;

/** Income multiplier: maximum loan is 25× annual income. */
export const INCOME_MULTIPLIER = 25;

/** Ratio threshold for the "affordable" tier (inclusive). */
export const AFFORDABLE_THRESHOLD = 90;

/** Ratio threshold for the "stretch" tier (inclusive lower bound). */
export const STRETCH_THRESHOLD = 60;

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Computes the affordability of a district for a given buyer profile.
 *
 * @param districtMedianPrice  Median total property price for the district (NT$).
 * @param monthlyIncome        Monthly household income (NT$).
 * @param downPayment          Saved down payment (NT$).
 * @returns AffordabilityResult, or null when any input is invalid (≤ 0 or non-finite).
 */
export function computeAffordability(
  districtMedianPrice: number,
  monthlyIncome: number,
  downPayment: number,
): AffordabilityResult | null {
  if (!Number.isFinite(districtMedianPrice) || districtMedianPrice <= 0) return null;
  if (!Number.isFinite(monthlyIncome) || monthlyIncome <= 0) return null;
  if (!Number.isFinite(downPayment) || downPayment <= 0) return null;

  const maxByDownPayment = downPayment / DOWN_PAYMENT_RATIO;
  const maxByIncome = monthlyIncome * 12 * INCOME_MULTIPLIER;
  const maxAffordable = Math.min(maxByDownPayment, maxByIncome);

  const ratio = (maxAffordable / districtMedianPrice) * 100;

  const tier: AffordabilityTier =
    ratio >= AFFORDABLE_THRESHOLD
      ? "affordable"
      : ratio >= STRETCH_THRESHOLD
        ? "stretch"
        : "outOfRange";

  const gap = districtMedianPrice - maxAffordable;

  return {
    maxAffordable: Math.round(maxAffordable),
    ratio: parseFloat(ratio.toFixed(2)),
    tier,
    gap: Math.round(gap),
  };
}

/**
 * Returns the emoji + Chinese label for an affordability tier.
 * Useful for tooltip and legend rendering.
 */
export function tierLabel(tier: AffordabilityTier): string {
  switch (tier) {
    case "affordable":
      return "🟢 可負擔";
    case "stretch":
      return "🟡 勉強可負擔";
    case "outOfRange":
      return "🔴 超出能力範圍";
  }
}

/**
 * Formats the gap as a Chinese readout suitable for a tooltip.
 * Positive gap  → "尚差 X 萬" (shortfall).
 * Negative gap  → "還有 X 萬空間" (surplus).
 * Zero gap      → "剛好可負擔".
 */
export function formatGap(gapNTD: number): string {
  const gapWan = Math.abs(gapNTD) / 10_000;
  if (gapNTD > 0) return `尚差 ${gapWan.toFixed(0)} 萬`;
  if (gapNTD < 0) return `還有 ${gapWan.toFixed(0)} 萬空間`;
  return "剛好可負擔";
}
