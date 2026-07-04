/**
 * Pure utility functions for the 相似地區推薦 (similar-district recommender) feature (issue #141).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Given a target district, computes the top N value-alternatives: districts that share a
 * similar buyer-timing score, affordability tier, and building-age profile, but have a
 * lower median price-per-ping (medianPricePing).
 *
 * All inputs are pre-computed district profiles derived from in-memory GeoJSON/transaction data.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

/**
 * Building-age bucket index (0–5), matching the BUCKET_DEFS order in buildingAgeUtils:
 *   0 = < 5年  |  1 = 5–10年  |  2 = 10–20年  |  3 = 20–30年  |  4 = 30–40年  |  5 = > 40年
 */
export type AgeBucketIndex = 0 | 1 | 2 | 3 | 4 | 5;

/**
 * First-buyer affordability tier (matches GeoJSON `firstBuyerTier` property):
 *   -1 = no data  |  0 = affordable (🟢)  |  1 = stretch (🟡)  |  2 = outOfRange (🔴)
 * Lower number → more affordable.
 */
export type FirstBuyerTierValue = -1 | 0 | 1 | 2;

/**
 * Flattened district profile — all dimensions needed for similarity computation.
 * Built by the Map component from pre-computed GeoJSON properties and rawAllFeatures.
 */
export interface DistrictProfile {
  /** Administrative district name, e.g. "信義區". */
  district: string;
  /** City name, e.g. "台北市". */
  city: string;
  /** Median price per ping (坪) in NT$. -1 = insufficient data. */
  medianPricePing: number;
  /** Buyer-timing score (0–100). null = insufficient transaction history. */
  buyerTimingScore: number | null;
  /** First-buyer affordability tier. -1 = no data. */
  firstBuyerTier: FirstBuyerTierValue;
  /**
   * Index of the dominant (highest-count) age bucket in this district's transaction history.
   * null = insufficient building-year data.
   */
  dominantAgeBucket: AgeBucketIndex | null;
}

/** A single candidate result from `computeSimilarDistricts`. */
export interface SimilarDistrictCandidate {
  /** Candidate district name. */
  district: string;
  /** Candidate city name. */
  city: string;
  /** Candidate median price per ping in NT$. */
  medianPricePing: number;
  /**
   * Price discount relative to the target district, as a percentage (0–100).
   * discountPct = (target.medianPricePing − candidate.medianPricePing) / target.medianPricePing × 100
   * Always positive (candidates are filtered to be cheaper than the target).
   */
  discountPct: number;
  /** Candidate buyer-timing score (0–100) or null. */
  buyerTimingScore: number | null;
  /** Candidate first-buyer affordability tier. */
  firstBuyerTier: FirstBuyerTierValue;
}

/** Return value of `computeSimilarDistricts`. */
export interface SimilarDistrictResult {
  /** Top similar-district candidates (up to `options.maxResults`, default 3). */
  candidates: SimilarDistrictCandidate[];
  /**
   * True when fewer than 3 qualifying candidates were found across all districts.
   * The UI should display a 資料不足 fallback message in this case.
   */
  insufficient: boolean;
}

/** Tuning options for `computeSimilarDistricts`. */
export interface SimilarDistrictOptions {
  /**
   * When true, only candidates in the same city as the target are considered.
   * @default false
   */
  sameCityOnly?: boolean;
  /**
   * Maximum absolute difference in buyer-timing score allowed.
   * Dimension is skipped (not filtered) when either party's score is null.
   * @default 15
   */
  timingScoreTolerance?: number;
  /**
   * Maximum number of candidates to return.
   * @default 3
   */
  maxResults?: number;
}

// ── Constants ─────────────────────────────────────────────────────────────────

/** Default timing-score tolerance (±15 points). */
export const DEFAULT_TIMING_TOLERANCE = 15;

/** Default max candidates returned. */
export const DEFAULT_MAX_RESULTS = 3;

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Derives the dominant age-bucket index from a median building age (years).
 * Used when converting GeoJSON `medianAge` property into an `AgeBucketIndex`.
 */
export function ageBucketFromMedianAge(medianAge: number): AgeBucketIndex {
  if (medianAge < 5)  return 0;
  if (medianAge < 10) return 1;
  if (medianAge < 20) return 2;
  if (medianAge < 30) return 3;
  if (medianAge < 40) return 4;
  return 5;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Computes the top value-alternative districts for a given target district.
 *
 * Similarity dimensions (all checked independently; dimensions with missing data are skipped):
 * 1. **Buyer-timing score**: |candidate − target| ≤ timingScoreTolerance (default 15).
 * 2. **Affordability tier**: candidate.firstBuyerTier ≤ target.firstBuyerTier
 *    (same or more affordable; lower tier number = better).
 * 3. **Dominant age bucket**: |candidate − target| ≤ 1.
 *
 * Candidates must also have medianPricePing < target.medianPricePing (strictly cheaper).
 *
 * @param targetDistrict  District name of the selected district.
 * @param targetCity      City of the selected district (used to resolve same-name districts).
 * @param allProfiles     All district profiles available in the current dataset.
 * @param options         Optional tuning parameters.
 * @returns               `SimilarDistrictResult` with top candidates and an insufficient flag.
 */
export function computeSimilarDistricts(
  targetDistrict: string,
  targetCity: string,
  allProfiles: DistrictProfile[],
  options?: SimilarDistrictOptions,
): SimilarDistrictResult {
  const tolerance = options?.timingScoreTolerance ?? DEFAULT_TIMING_TOLERANCE;
  const maxResults = options?.maxResults ?? DEFAULT_MAX_RESULTS;
  const sameCityOnly = options?.sameCityOnly ?? false;

  // Find the target profile (city-aware; fall back to district-only when city is empty)
  const target =
    allProfiles.find(
      (p) => p.district === targetDistrict && p.city === targetCity,
    ) ??
    (targetCity === ""
      ? allProfiles.find((p) => p.district === targetDistrict)
      : undefined);

  // Can't compute without a valid target with price data
  if (!target || target.medianPricePing <= 0) {
    return { candidates: [], insufficient: true };
  }

  const qualifying: SimilarDistrictCandidate[] = [];

  for (const candidate of allProfiles) {
    // Exclude the target itself (match on district+city; when city is "", exclude all same-district rows)
    const isTarget =
      candidate.district === targetDistrict &&
      (targetCity === "" ? true : candidate.city === targetCity);
    if (isTarget) continue;

    // Exclude entries with no price data
    if (candidate.medianPricePing <= 0) continue;

    // Candidate must be strictly cheaper
    if (candidate.medianPricePing >= target.medianPricePing) continue;

    // Optional same-city filter
    if (sameCityOnly && candidate.city !== target.city) continue;

    // ── Dimension 1: Buyer-timing score (skip when either is null) ──────────
    if (
      target.buyerTimingScore !== null &&
      candidate.buyerTimingScore !== null &&
      Math.abs(candidate.buyerTimingScore - target.buyerTimingScore) > tolerance
    ) {
      continue;
    }

    // ── Dimension 2: Affordability tier (skip when either has no data) ─────
    // Lower tier = more affordable; candidate must be same or more affordable.
    if (
      target.firstBuyerTier >= 0 &&
      candidate.firstBuyerTier >= 0 &&
      candidate.firstBuyerTier > target.firstBuyerTier
    ) {
      continue;
    }

    // ── Dimension 3: Dominant age bucket (skip when either is null) ─────────
    if (
      target.dominantAgeBucket !== null &&
      candidate.dominantAgeBucket !== null &&
      Math.abs(candidate.dominantAgeBucket - target.dominantAgeBucket) > 1
    ) {
      continue;
    }

    // Compute discount percentage
    const discountPct = parseFloat(
      (
        ((target.medianPricePing - candidate.medianPricePing) / target.medianPricePing) *
        100
      ).toFixed(1),
    );

    qualifying.push({
      district: candidate.district,
      city: candidate.city,
      medianPricePing: candidate.medianPricePing,
      discountPct,
      buyerTimingScore: candidate.buyerTimingScore,
      firstBuyerTier: candidate.firstBuyerTier,
    });
  }

  // Rank by discount descending (largest discount first)
  qualifying.sort((a, b) => b.discountPct - a.discountPct);

  const insufficient = qualifying.length < 3;
  const candidates = qualifying.slice(0, maxResults);

  return { candidates, insufficient };
}
