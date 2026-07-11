/**
 * Pure utility functions for district facilities (POI) scoring (issue #171).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Data source: OpenStreetMap Overpass API, fetched at build time via
 * scripts/fetch-poi-data.mjs and baked into public/data/poi-scores.json.
 *
 * Scoring: weighted sum of POI category counts, normalized 0–100.
 * Higher = richer daily-life amenities.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

/** Raw POI category counts per district (from poi-scores.json). */
export interface PoiCounts {
  restaurant: number;    // 餐廳
  sport: number;         // 運動中心/健身房
  cinema: number;        // 電影院
  supermarket: number;   // 超市
  convenience: number;   // 超商
  park: number;          // 公園/綠地
  hospital: number;      // 醫院/診所
}

/** Derived facilities score with label. */
export interface FacilitiesResult {
  score: number;        // 0–100 normalized facilities score
  counts: PoiCounts;
}

// ── Category weights ──────────────────────────────────────────────────────────

/**
 * Weight each category contributes to the facilities score.
 * Higher weight = more impact on livability for typical household.
 */
const CATEGORY_WEIGHTS: Record<keyof PoiCounts, number> = {
  convenience: 3.0,   // 超商 — daily essentials, density matters most
  restaurant:  2.5,   // 餐廳 — dining variety
  supermarket: 2.5,   // 超市 — grocery access
  park:        2.0,   // 公園 — green space
  hospital:    2.0,   // 醫院/診所 — healthcare
  sport:       1.5,   // 運動中心/健身房
  cinema:      0.5,   // 電影院 — entertainment
};

/**
 * Soft cap per category: counts above this contribute with diminishing returns
 * so that a district with 500 restaurants doesn't dominate purely on volume.
 */
const CATEGORY_SOFT_CAP: Record<keyof PoiCounts, number> = {
  convenience: 80,
  restaurant:  120,
  supermarket: 30,
  park:        40,
  hospital:    60,
  sport:       20,
  cinema:      5,
};

// ── Score normalization ────────────────────────────────────────────────────────

/**
 * Compute a raw (un-normalized) weighted score for a single district.
 * Uses a soft-cap log formula: weight × min(count, cap) + weight × log(extra+1)
 * where extra = max(0, count - cap).
 */
export function rawFacilitiesScore(counts: PoiCounts): number {
  let total = 0;
  for (const cat of Object.keys(CATEGORY_WEIGHTS) as (keyof PoiCounts)[]) {
    const w = CATEGORY_WEIGHTS[cat];
    const cap = CATEGORY_SOFT_CAP[cat];
    const count = counts[cat] ?? 0;
    const capped = Math.min(count, cap);
    const extra = Math.max(0, count - cap);
    total += w * capped + w * Math.log1p(extra) * 5;
  }
  return total;
}

/**
 * Normalize an array of raw scores to a 0–100 scale.
 * The district with the highest raw score gets 100; the lowest gets 0.
 * Falls back to 50 when all districts have the same score.
 */
export function normalizeScores(rawScores: number[]): number[] {
  if (rawScores.length === 0) return [];
  const min = Math.min(...rawScores);
  const max = Math.max(...rawScores);
  if (max === min) return rawScores.map(() => 50);
  return rawScores.map(s => Math.round(((s - min) / (max - min)) * 100));
}

/**
 * Compute normalized facilities scores for a batch of districts.
 * Input: map from `city::district` key → PoiCounts
 * Output: same keys → 0–100 normalized score
 */
export function computeAllFacilitiesScores(
  poiData: Record<string, PoiCounts>,
): Record<string, number> {
  const keys = Object.keys(poiData);
  const raws = keys.map(k => rawFacilitiesScore(poiData[k]!));
  const normalized = normalizeScores(raws);
  const result: Record<string, number> = {};
  keys.forEach((k, i) => { result[k] = normalized[i]!; });
  return result;
}

// ── Label / emoji helpers ─────────────────────────────────────────────────────

export type FacilitiesTier = "excellent" | "good" | "fair" | "poor";

/** Classify a 0–100 facilities score into a tier. */
export function facilitiesTier(score: number): FacilitiesTier {
  if (score >= 75) return "excellent";
  if (score >= 50) return "good";
  if (score >= 25) return "fair";
  return "poor";
}

export const FACILITIES_TIER_LABELS: Record<FacilitiesTier, string> = {
  excellent: "生活機能優",
  good:      "生活機能良",
  fair:      "生活機能普",
  poor:      "生活機能待加強",
};

export const FACILITIES_TIER_EMOJIS: Record<FacilitiesTier, string> = {
  excellent: "🟢",
  good:      "🟡",
  fair:      "🟠",
  poor:      "🔴",
};

/** One-liner badge text, e.g. "🟢 生活機能優 (85)". */
export function facilitiesBadge(score: number): string {
  const tier = facilitiesTier(score);
  return `${FACILITIES_TIER_EMOJIS[tier]} ${FACILITIES_TIER_LABELS[tier]} (${score})`;
}
