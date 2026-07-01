/**
 * Pure utility functions for the first-time buyer district match wizard (issue #114).
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * Wizard flow:
 *   Step 1 – Budget (total price tier + down-payment readiness)
 *   Step 2 – Commute hub + acceptable commute time
 *   Step 3 – Priority (lowest price / best ping efficiency / newest / highest buyer score)
 *
 * Output: top-3 matching districts + `needsRelax` flag when fewer than 3 qualify.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

export type BudgetTier = "under-800" | "800-1500" | "1500-2500" | "over-2500";
export type DownPaymentTier = "under-5" | "5-10" | "10-20";
export type CommuteMins = 30 | 45 | 60;
export type Priority = "total-lowest" | "ping-best" | "newest" | "timing-score";

export interface CommuteHub {
  id: string;
  label: string;
  lat: number;
  lng: number;
}

/** Per-district data required by the wizard matching algorithm. */
export interface DistrictInput {
  district: string;
  city: string;
  medianPriceWan: number | null;  // 萬/坪 median unit price (last 12 mo)
  yoyPct: number | null;          // year-over-year % price change
  txCount12mo: number;            // transaction count in last 12 months
  buyerTimingScore: number | null; // 0–100 buyer timing score
  lat: number;                    // district centroid latitude
  lng: number;                    // district centroid longitude
}

export interface WizardParams {
  budget: BudgetTier;
  downPayment: DownPaymentTier;
  commuteHub: string;   // hub id from COMMUTE_HUBS
  commuteMins: CommuteMins;
  priority: Priority;
}

export interface WizardMatch {
  district: string;
  city: string;
  medianPriceWan: number | null;
  buyerTimingScore: number | null;
  distanceKm: number;
  commuteHub: string;
  hubLabel: string;
}

export interface WizardOutput {
  matches: WizardMatch[];
  needsRelax: boolean; // true when fewer than 3 districts survived all filters
}

// ── Static data ───────────────────────────────────────────────────────────────

/** Major commute hubs with approximate coordinates. */
export const COMMUTE_HUBS: CommuteHub[] = [
  { id: "taipei-main",   label: "台北車站",   lat: 25.0478, lng: 121.5170 },
  { id: "xinyi",         label: "信義區",     lat: 25.0330, lng: 121.5655 },
  { id: "banqiao",       label: "板橋",       lat: 25.0142, lng: 121.4633 },
  { id: "hsinchu",       label: "新竹市",     lat: 24.8036, lng: 120.9716 },
  { id: "taichung-main", label: "台中火車站", lat: 24.1367, lng: 120.6842 },
  { id: "kaohsiung-mrt", label: "高雄捷運",   lat: 22.6273, lng: 120.3014 },
];

/**
 * Budget tier → approximate median unit-price range (萬/坪).
 * Derived from 30-坪 typical apartment assumption.
 * min is inclusive, max is exclusive (null = no upper bound).
 */
export const BUDGET_PRICE_RANGES: Record<BudgetTier, { min: number; max: number | null }> = {
  "under-800":  { min: 0,    max: 26.7 },
  "800-1500":   { min: 0,    max: 50.0 },  // inclusive lower end retained so 800–1500 range includes <800
  "1500-2500":  { min: 26.7, max: 83.4 },
  "over-2500":  { min: 83.4, max: null  },
};

// ── Geometry ──────────────────────────────────────────────────────────────────

/**
 * Haversine great-circle distance in kilometres.
 * Accepts decimal degrees (WGS-84).
 */
export function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Estimate straight-line commute time in minutes.
 * Uses an effective speed of 30 km/h (urban mix of walking + transit).
 */
export function estimateCommuteMins(distanceKm: number): number {
  return distanceKm / 30 * 60;
}

/**
 * Maximum straight-line distance (km) for a given commute time budget.
 * Inverse of estimateCommuteMins.
 */
export function maxDistanceForMins(mins: CommuteMins): number {
  return (mins / 60) * 30;
}

// ── Polygon centroid helper ────────────────────────────────────────────────────

/**
 * Compute the centroid (average lat/lng) of a GeoJSON Polygon or MultiPolygon.
 * Used by the page layer to attach centroids to DistrictInput objects.
 * Returns null when geometry is invalid.
 */
export function polygonCentroid(
  geometry: { type: string; coordinates: any }
): { lat: number; lng: number } | null {
  if (!geometry) return null;
  let rings: [number, number][][] = [];

  if (geometry.type === "Polygon") {
    rings = geometry.coordinates as [number, number][][];
  } else if (geometry.type === "MultiPolygon") {
    for (const poly of geometry.coordinates as [number, number][][][]) {
      rings.push(...poly);
    }
  } else {
    return null;
  }

  if (rings.length === 0) return null;
  const outer = rings[0]!;
  if (outer.length === 0) return null;

  // GeoJSON polygon rings are closed (last coord == first coord). Skip the closing vertex.
  const pts =
    outer.length > 1 &&
    outer[0]![0] === outer[outer.length - 1]![0] &&
    outer[0]![1] === outer[outer.length - 1]![1]
      ? outer.slice(0, -1)
      : outer;

  let sumLng = 0;
  let sumLat = 0;
  let count = 0;
  for (const [lng, lat] of pts) {
    if (typeof lng === "number" && typeof lat === "number") {
      sumLng += lng;
      sumLat += lat;
      count++;
    }
  }
  if (count === 0) return null;
  return { lat: sumLat / count, lng: sumLng / count };
}

// ── Filtering ─────────────────────────────────────────────────────────────────

/**
 * Filter districts by budget tier.
 * Districts with null medianPriceWan are excluded (insufficient data).
 */
export function filterByBudget(
  districts: DistrictInput[],
  budget: BudgetTier
): DistrictInput[] {
  const { min, max } = BUDGET_PRICE_RANGES[budget];
  return districts.filter((d) => {
    if (d.medianPriceWan === null) return false;
    if (d.medianPriceWan < min) return false;
    if (max !== null && d.medianPriceWan >= max) return false;
    return true;
  });
}

/**
 * Filter districts by straight-line commute distance to a hub.
 * Districts whose centroid is within `maxDistanceForMins(commuteMins)` km qualify.
 */
export function filterByCommute(
  districts: DistrictInput[],
  hubId: string,
  commuteMins: CommuteMins
): DistrictInput[] {
  const hub = COMMUTE_HUBS.find((h) => h.id === hubId);
  if (!hub) return districts; // unknown hub → don't filter
  const maxKm = maxDistanceForMins(commuteMins);
  return districts.filter((d) => {
    const km = haversineKm(d.lat, d.lng, hub.lat, hub.lng);
    return km <= maxKm;
  });
}

// ── Sorting ───────────────────────────────────────────────────────────────────

/**
 * Sort districts by the chosen priority.
 * Null values always sort last.
 */
export function sortByPriority(
  districts: DistrictInput[],
  priority: Priority
): DistrictInput[] {
  return [...districts].sort((a, b) => {
    switch (priority) {
      case "total-lowest":
      case "ping-best": {
        // Both map to lowest median unit price
        const av = a.medianPriceWan;
        const bv = b.medianPriceWan;
        if (av === null && bv === null) return 0;
        if (av === null) return 1;
        if (bv === null) return -1;
        return av - bv; // ascending (cheapest first)
      }
      case "newest": {
        // Proxy: higher txCount12mo → more active = more likely newer supply
        return b.txCount12mo - a.txCount12mo; // descending
      }
      case "timing-score": {
        const av = a.buyerTimingScore;
        const bv = b.buyerTimingScore;
        if (av === null && bv === null) return 0;
        if (av === null) return 1;
        if (bv === null) return -1;
        return bv - av; // descending (highest score first)
      }
    }
  });
}

// ── Main wizard function ───────────────────────────────────────────────────────

/**
 * Run the full wizard matching pipeline.
 * Returns top 3 matches + needsRelax flag.
 */
export function runWizard(params: WizardParams, districts: DistrictInput[]): WizardOutput {
  const hub = COMMUTE_HUBS.find((h) => h.id === params.commuteHub);

  let filtered = filterByBudget(districts, params.budget);
  filtered = filterByCommute(filtered, params.commuteHub, params.commuteMins);
  filtered = sortByPriority(filtered, params.priority);

  const top3 = filtered.slice(0, 3);
  const needsRelax = top3.length < 3;

  const matches: WizardMatch[] = top3.map((d) => ({
    district: d.district,
    city: d.city,
    medianPriceWan: d.medianPriceWan,
    buyerTimingScore: d.buyerTimingScore,
    distanceKm: hub
      ? Math.round(haversineKm(d.lat, d.lng, hub.lat, hub.lng) * 10) / 10
      : 0,
    commuteHub: params.commuteHub,
    hubLabel: hub?.label ?? params.commuteHub,
  }));

  return { matches, needsRelax };
}
