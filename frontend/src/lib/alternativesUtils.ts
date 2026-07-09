/**
 * Pure utility functions for the 智慧替代方案推薦 (smart alternatives) feature (issue #163).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Given a target district and all loaded district profiles (from districtGeoJSON),
 * computes the top cheaper same-city alternatives sorted by ascending median unit price.
 *
 * MRT tier indicator is included when a mrtTierMap is supplied (from Map's cachedMrtProximity).
 */

import type { MrtTierValue } from "./mrtProximityUtils.js";
import type { DistrictProfile } from "./similarDistrictUtils.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface AlternativeCandidate {
  /** District name, e.g. "文山區". */
  district: string;
  /** City name, e.g. "台北市". */
  city: string;
  /** Median price per ping (坪) in NT$. */
  medianPricePing: number;
  /**
   * Percentage cheaper vs the selected district (positive, rounded to 1 decimal).
   * e.g. 26.4 means "26% cheaper".
   */
  priceDeltaPct: number;
  /**
   * MRT proximity tier for this district, or null when no station data is available.
   */
  mrtTier: MrtTierValue | null;
  /** True when this district has no MRT coverage at all (noMrt flag from proximity result). */
  noMrtCoverage: boolean;
}

export interface AlternativesResult {
  /** Top cheaper same-city alternatives (up to `maxResults`). */
  candidates: AlternativeCandidate[];
  /**
   * True when the target district itself had insufficient data (medianPricePing ≤ 0),
   * making comparison impossible.
   */
  targetInsufficient: boolean;
}

// ── Constants ──────────────────────────────────────────────────────────────────

export const ALTERNATIVES_MAX_RESULTS = 5;

// ── Core computation ───────────────────────────────────────────────────────────

/**
 * Compute top N cheaper same-city districts vs the selected district.
 *
 * Filtering rules:
 *  - Must be in the same city as the target district.
 *  - Must NOT be the target district itself.
 *  - Must have valid medianPricePing (> 0).
 *  - Must be strictly cheaper than the target.
 *
 * Results are sorted by ascending medianPricePing (cheapest first).
 *
 * @param targetDistrict  Selected district name.
 * @param targetCity      Selected city name.
 * @param allProfiles     All district profiles (from districtGeoJSON + rawAllFeatures).
 * @param mrtTierMap      Optional map of "city:district" → { tier, noMrt } from cachedMrtProximity.
 * @param maxResults      Maximum number of results to return (default: ALTERNATIVES_MAX_RESULTS).
 */
export function computeAlternatives(
  targetDistrict: string,
  targetCity: string,
  allProfiles: DistrictProfile[],
  mrtTierMap: Record<string, { tier: MrtTierValue; noMrt: boolean }> = {},
  maxResults: number = ALTERNATIVES_MAX_RESULTS,
): AlternativesResult {
  // Clamp maxResults to a safe integer (guards against negative or fractional callers)
  const limit = Math.max(0, Math.floor(maxResults));
  // Find the target profile
  const target = allProfiles.find(
    (p) => p.district === targetDistrict && p.city === targetCity,
  );

  if (!target || target.medianPricePing <= 0) {
    // Both "district not in profiles" and "district has no price data" are treated as
    // targetInsufficient because neither can serve as a price baseline for comparison.
    return { candidates: [], targetInsufficient: true };
  }

  const targetPrice = target.medianPricePing;

  const candidates: AlternativeCandidate[] = allProfiles
    .filter(
      (p) =>
        p.city === targetCity &&
        p.district !== targetDistrict &&
        p.medianPricePing > 0 &&
        p.medianPricePing < targetPrice,
    )
    .sort((a, b) => a.medianPricePing - b.medianPricePing)
    .slice(0, limit)
    .map((p) => {
      const key = `${p.city}:${p.district}`;
      const mrtEntry = mrtTierMap[key] ?? null;
      const priceDeltaPct =
        Math.round(((targetPrice - p.medianPricePing) / targetPrice) * 1000) / 10;
      return {
        district: p.district,
        city: p.city,
        medianPricePing: p.medianPricePing,
        priceDeltaPct,
        mrtTier: mrtEntry ? mrtEntry.tier : null,
        noMrtCoverage: mrtEntry ? mrtEntry.noMrt : false,
      };
    });

  return { candidates, targetInsufficient: false };
}
