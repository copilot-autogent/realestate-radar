/**
 * Pure utility functions for MRT (捷運) proximity scoring (issue #144).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Distance tiers (straight-line Haversine):
 *   🟢 步行圈  (walking)  ≤ 0.5 km
 *   🟡 生活圈  (nearby)   0.5 – 1.0 km
 *   🟠 需搭車  (driving)  1.0 – 2.0 km
 *   🔴 偏遠    (remote)   > 2.0 km
 *
 * When station list is empty the result has tier='remote', nearestStation=null,
 * and noMrt=true — callers should show 無捷運 rather than a tier badge.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

export interface MrtStation {
  id: string;
  name: string;
  lat: number;
  lng: number;
  line: string;
  lineZh: string;
}

export type MrtTierValue = "walking" | "nearby" | "driving" | "remote";

export interface MrtProximityResult {
  /** Nearest station, or null when station list is empty. */
  nearestStation: MrtStation | null;
  /** Straight-line distance to nearest station in km. Infinity when no stations. */
  distanceKm: number;
  /** Convenience tier derived from distanceKm. */
  tier: MrtTierValue;
  /** Estimated walking time in minutes (assumes 80 m/min). Null when no stations. */
  walkMinutes: number | null;
  /** True when station list is empty (cities with no MRT coverage). */
  noMrt: boolean;
}

// ── Constants (UI helpers) ────────────────────────────────────────────────────

export const MRT_TIER_LABELS: Record<MrtTierValue, string> = {
  walking: "步行圈",
  nearby:  "生活圈",
  driving: "需搭車",
  remote:  "偏遠",
};

export const MRT_TIER_EMOJIS: Record<MrtTierValue, string> = {
  walking: "🟢",
  nearby:  "🟡",
  driving: "🟠",
  remote:  "🔴",
};

/** MapLibre-compatible fill colours for choropleth. */
export const MRT_TIER_COLORS: Record<MrtTierValue | "noMrt", string> = {
  walking: "#16a34a",
  nearby:  "#ca8a04",
  driving: "#ea580c",
  remote:  "#dc2626",
  noMrt:   "#94a3b8",
};

// ── Pure maths ────────────────────────────────────────────────────────────────

/**
 * Haversine great-circle distance between two [lng, lat] coordinate pairs.
 * Returns distance in kilometres.
 */
export function haversineDistanceKm(
  [lng1, lat1]: [number, number],
  [lng2, lat2]: [number, number],
): number {
  const R = 6371; // Earth radius in km
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Classify a distance (km) into a transit convenience tier.
 * Boundaries: ≤0.5 = walking, ≤1.0 = nearby, ≤2.0 = driving, >2.0 = remote.
 */
export function mrtTier(distanceKm: number): MrtTierValue {
  if (distanceKm <= 0.5) return "walking";
  if (distanceKm <= 1.0) return "nearby";
  if (distanceKm <= 2.0) return "driving";
  return "remote";
}

/**
 * Simple arithmetic centroid of a GeoJSON polygon outer ring.
 * Input: flat array of [lng, lat] pairs (outer ring of a Polygon).
 * Returns [lng, lat] centroid.
 * The last coordinate is typically identical to the first (closed ring); both
 * are included in the average — the effect is negligible and avoids special-casing.
 */
export function polygonCentroid(coordinates: [number, number][]): [number, number] {
  if (coordinates.length === 0) return [0, 0];
  const lng = coordinates.reduce((s, c) => s + c[0], 0) / coordinates.length;
  const lat = coordinates.reduce((s, c) => s + c[1], 0) / coordinates.length;
  return [lng, lat];
}

// ── Main API ──────────────────────────────────────────────────────────────────

/**
 * Compute MRT proximity from a district centroid to the nearest station.
 *
 * @param districtCentroid - [lng, lat] of the district centroid.
 * @param stations         - Array of MRT stations (may be empty for cities without MRT).
 * @returns MrtProximityResult with tier, distance, nearest station, and walk estimate.
 */
export function computeMrtProximity(
  districtCentroid: [number, number],
  stations: MrtStation[],
): MrtProximityResult {
  if (stations.length === 0) {
    return {
      nearestStation: null,
      distanceKm: Infinity,
      tier: "remote",
      walkMinutes: null,
      noMrt: true,
    };
  }

  let nearestStation: MrtStation = stations[0]!;
  let minDistKm = haversineDistanceKm(districtCentroid, [stations[0]!.lng, stations[0]!.lat]);

  for (let i = 1; i < stations.length; i++) {
    const st = stations[i]!;
    const d = haversineDistanceKm(districtCentroid, [st.lng, st.lat]);
    if (d < minDistKm) {
      minDistKm = d;
      nearestStation = st;
    }
  }

  // Walking speed: 80 m/min ≈ 4.8 km/h (typical city pedestrian pace)
  const WALK_SPEED_KM_PER_MIN = 0.08;
  const walkMinutes = Math.round(minDistKm / WALK_SPEED_KM_PER_MIN);

  return {
    nearestStation,
    distanceKm: minDistKm,
    tier: mrtTier(minDistKm),
    walkMinutes,
    noMrt: false,
  };
}
