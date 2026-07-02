/**
 * Pure utility functions for the supply-demand pressure overlay (issue #123).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Formula:
 *   SupplyPressure = (upcoming_completions_per_1000_residents) × (1 / buyer_timing_score) × velocity_weight
 *
 * Tiers: low | medium-low | medium | medium-high | high (5-tier green→red gradient)
 *
 * IMPORTANT: Callers passing in district or city strings from Taiwan government
 * data sources must normalize full-width digits first:
 *   rawString.normalize('NFKC')
 * (Taiwan gov data uses full-width digits U+FF10–FF19 in addresses and identifiers.)
 */

export type SupplyPressureTier = "low" | "medium-low" | "medium" | "medium-high" | "high";

/** zh-TW display labels for each tier (used in tooltips and legends). */
export const TIER_LABEL_ZH: Record<SupplyPressureTier, string> = {
  "low":         "低",
  "medium-low":  "中低",
  "medium":      "中",
  "medium-high": "中高",
  "high":        "高",
};

/** Minimum number of completion data records required to compute a pressure signal. */
export const MIN_COMPLETION_RECORDS = 5;

const DEFAULT_POPULATION = 30_000;

/**
 * Score scale factor: raw formula value is multiplied by this to produce a 0–100 score.
 * Calibrated so a district with ~30 completions/1000 residents, buyer score 50,
 * flat velocity lands in the medium tier (~score 20).
 */
const SCORE_SCALE = 33;

export interface DistrictSupplyDemand {
  district: string;
  city?: string;
  /** Upcoming completion units (2025–2027) allocated to this district. */
  upcomingCompletions: number;
  /**
   * Number of valid historical permit data records backing the completion estimate.
   * Districts with fewer than MIN_COMPLETION_RECORDS fall back to "資料不足".
   */
  completionRecordCount: number;
  /**
   * District population estimate for per-1000-residents normalization.
   * Defaults to 30,000 when omitted or zero.
   */
  populationEstimate?: number;
  /** Transactions in the most recent 12 months (velocity numerator). */
  txCount12mo: number;
  /** Transactions in the prior 12 months (12–24 months ago). Used for velocity weight. */
  txCountPrior12mo?: number;
  /**
   * Buyer timing score 0–100, or null when insufficient transaction data.
   * Low score = seller's market conditions.
   */
  buyerTimingScore: number | null;
}

export interface SupplyPressureResult {
  tier: SupplyPressureTier;
  /** zh-TW display label for the tier */
  tierLabel: string;
  /** True when completionRecordCount < MIN_COMPLETION_RECORDS — caller should show 資料不足 */
  insufficient: boolean;
  /** Normalized score 0–100 */
  score: number;
  /** Completions per 1000 residents (first component of formula) */
  completionsPer1000: number;
  /** Velocity weight applied: 0.7 (rising demand), 1.0 (flat), or 1.3 (falling demand) */
  velocityWeight: number;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

/**
 * Derive velocity weight from recent vs prior 12-month transaction counts.
 * Falling demand amplifies supply pressure; rising demand dampens it.
 *
 * - ratio < 0.8  → falling  → weight 1.3
 * - ratio > 1.2  → rising   → weight 0.7
 * - otherwise    → flat     → weight 1.0
 *
 * Returns 1.0 when prior count is unavailable or zero.
 */
function computeVelocityWeight(txCount12mo: number, txCountPrior12mo?: number): number {
  if (txCountPrior12mo == null || txCountPrior12mo <= 0) return 1.0;
  if (txCount12mo <= 0) return 1.3; // zero current demand with prior demand = falling
  const ratio = txCount12mo / txCountPrior12mo;
  if (ratio < 0.8) return 1.3;
  if (ratio > 1.2) return 0.7;
  return 1.0;
}

/** Map a normalized 0–100 score to a supply pressure tier. */
function scoreToTier(score: number): SupplyPressureTier {
  if (score < 20) return "low";
  if (score < 40) return "medium-low";
  if (score < 60) return "medium";
  if (score < 80) return "medium-high";
  return "high";
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Computes the supply-demand pressure tier for a district.
 *
 * Formula: SupplyPressure = completionsPer1000 × (1 / buyer_timing_score) × velocity_weight
 *
 * Returns `insufficient: true` when `completionRecordCount < MIN_COMPLETION_RECORDS`.
 *
 * Graceful fallbacks:
 * - buyerTimingScore === null → treated as 50 (neutral market)
 * - buyerTimingScore === 0    → clamped to 1 (avoids division by zero)
 * - populationEstimate === 0  → uses DEFAULT_POPULATION (30,000)
 * - txCount12mo === 0         → velocity weight 1.3 when prior > 0; 1.0 otherwise
 * - upcomingCompletions === 0 → score 0, tier "low"
 */
export function computeSupplyPressure(data: DistrictSupplyDemand): SupplyPressureResult {
  // Data-sufficiency gate
  if (data.completionRecordCount < MIN_COMPLETION_RECORDS) {
    return {
      tier: "low",
      tierLabel: TIER_LABEL_ZH["low"],
      insufficient: true,
      score: 0,
      completionsPer1000: 0,
      velocityWeight: 1.0,
    };
  }

  const population =
    data.populationEstimate != null && data.populationEstimate > 0
      ? data.populationEstimate
      : DEFAULT_POPULATION;

  // Component 1: upcoming completions per 1000 residents
  const completionsPer1000 =
    data.upcomingCompletions > 0
      ? (data.upcomingCompletions / population) * 1000
      : 0;

  // Component 2: 1 / buyer_timing_score
  // Clamped to [1, 100] to prevent division by zero.
  // Low buyer score (hot / seller's market) → large multiplier → higher pressure signal.
  const buyerScore =
    data.buyerTimingScore !== null ? clamp(data.buyerTimingScore, 1, 100) : 50;
  const inverseBuyerScore = 1 / buyerScore;

  // Component 3: velocity weight
  const velocityWeight = computeVelocityWeight(data.txCount12mo, data.txCountPrior12mo);

  // Raw score — product of three components
  const rawScore = completionsPer1000 * inverseBuyerScore * velocityWeight;

  // Normalize to 0–100
  const score = clamp(rawScore * SCORE_SCALE, 0, 100);
  const roundedScore = Math.round(score * 10) / 10;
  const tier = scoreToTier(roundedScore);

  return {
    tier,
    tierLabel: TIER_LABEL_ZH[tier],
    insufficient: false,
    score: roundedScore,
    completionsPer1000: Math.round(completionsPer1000 * 100) / 100,
    velocityWeight,
  };
}
