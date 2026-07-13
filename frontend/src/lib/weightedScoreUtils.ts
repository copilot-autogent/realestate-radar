/**
 * Pure utility functions for the tunable priority-weights scoring system (issue #172).
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * ## Scoring dimensions
 * Each district is scored on 5 independent dimensions (0–100):
 *
 * | Dimension   | Raw signal                                       | Direction |
 * |-------------|--------------------------------------------------|-----------|
 * | cost        | affordability: max 坪 within budget vs others   | higher→better |
 * | commute     | anchor proximity (exponential decay, 8 km half-life) | higher→better |
 * | space       | 坪 achievable within budget = budgetWan / medianPrice | higher→better |
 * | newness     | share of transactions < 20 years old (ratio 0–1) | higher→better |
 * | facilities  | POI-density weighted score (pre-normalized 0–100) | already normalized |
 *
 * ## Normalization method
 * All five raw values are normalized per cohort using **min–max scaling**:
 *   `score_i = (raw_i - min) / (max - min) × 100`
 * When all districts share the same raw value (max == min), all scores default to 50
 * (neutral, not 0) so a flat dimension doesn't zero-out the composite.
 *
 * ## Composite formula
 * `composite = Σ(weight_d × score_d) / Σ(weight_d)`
 * where weight_d is the user-tunable slider value (0–10) for each dimension d.
 * When all weights are 0 the composite is 0 for all districts.
 */

// ── Types ──────────────────────────────────────────────────────────────────────

/** Tunable weight for each scoring dimension. Values 0–10. */
export interface PriorityWeights {
  cost: number;       // 價格 / 負擔能力
  commute: number;    // 通勤便利性
  space: number;      // 可購坪數
  newness: number;    // 屋齡新穎度
  facilities: number; // 生活機能
}

/** Normalized 0–100 sub-score for each dimension (null = not computable). */
export interface DimensionScores {
  cost: number | null;
  commute: number | null;
  space: number | null;
  newness: number | null;
  facilities: number | null;
}

/** Per-district scoring result. */
export interface WeightedDistrictScore {
  district: string;
  city: string;
  composite: number;          // 0–100 weighted composite score
  dimensions: DimensionScores; // per-dimension sub-scores for "why" breakdown
}

/** Minimum data needed per district for scoring. */
export interface DistrictForScoring {
  district: string;
  city: string;
  medianPriceWan: number | null;  // median unit price (萬/坪)
  lat: number;                    // centroid latitude
  lng: number;                    // centroid longitude
  facilitiesScore: number | null; // 0–100 POI score from facilitiesUtils
  /** Features array for newness computation — may be empty. */
  features?: any[];
}

// ── Default weights ───────────────────────────────────────────────────────────

/** Equal-weight default (all 5 at 5). */
export const DEFAULT_WEIGHTS: PriorityWeights = {
  cost: 5,
  commute: 5,
  space: 5,
  newness: 5,
  facilities: 5,
};

// ── Geometry helper ───────────────────────────────────────────────────────────

function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Normalization ─────────────────────────────────────────────────────────────

/**
 * Min–max normalize an array of nullable numbers to 0–100.
 * - Null values map to null (data not available for that district).
 * - When all non-null values are equal (flat dimension), all map to 50.
 * - Higher raw value → higher normalized score (pass higherIsBetter=false to invert).
 */
export function minMaxNormalize(
  values: (number | null)[],
  higherIsBetter = true,
): (number | null)[] {
  const nonNull = values.filter((v): v is number => v !== null);
  if (nonNull.length === 0) return values.map(() => null);

  const min = Math.min(...nonNull);
  const max = Math.max(...nonNull);

  return values.map((v) => {
    if (v === null) return null;
    if (max === min) return 50; // flat dimension → neutral
    const normalized = ((v - min) / (max - min)) * 100;
    const score = higherIsBetter ? normalized : 100 - normalized;
    return Math.round(Math.max(0, Math.min(100, score)));
  });
}

// ── Raw dimension extractors ──────────────────────────────────────────────────

/**
 * Compute the raw "space" value: 坪 purchasable within budget at district's median unit price.
 * Higher = more space for the money = better.
 * Returns null when median price is unavailable or budget ≤ 0.
 */
export function rawSpaceValue(medianPriceWan: number | null, budgetWan: number): number | null {
  if (medianPriceWan === null || medianPriceWan <= 0 || budgetWan <= 0) return null;
  return budgetWan / medianPriceWan;
}

/**
 * Compute the raw "cost" (affordability) value: inverse of median unit price.
 * Lower price → higher affordability → higher raw value.
 * Returns null when median price is unavailable or budget ≤ 0.
 *
 * We use `budgetWan / medianPriceWan` capped to 1 as the raw affordability ratio,
 * but normalization will handle scale — so we just return the inverse price directly.
 */
export function rawCostValue(medianPriceWan: number | null): number | null {
  if (medianPriceWan === null || medianPriceWan <= 0) return null;
  // Lower price → better → return reciprocal so "higher is better" normalization works
  return 1 / medianPriceWan;
}

/**
 * Compute the raw "commute" score (0–100) using exponential decay from anchor.
 * Half-life = 8 km. Score 100 at 0 km, ~50 at 8 km, ~25 at 16 km.
 * Returns null when district centroid is at (0,0) (unresolved centroid).
 */
export function rawCommuteValue(
  distLat: number,
  distLng: number,
  anchorLat: number,
  anchorLng: number,
): number | null {
  if (distLat === 0 && distLng === 0) return null;
  const km = haversineKm(distLat, distLng, anchorLat, anchorLng);
  const HALF_LIFE_KM = 8;
  return Math.round(Math.pow(0.5, km / HALF_LIFE_KM) * 100);
}

/**
 * Compute the raw "newness" value: fraction of transactions < 20 years old.
 * Returns null when no features with parseable build years are found.
 * Range: 0–1 (1 = all transactions are < 20 years old).
 */
export function rawNewnessValue(features: any[], district: string, city: string): number | null {
  if (!features || features.length === 0) return null;

  const refYear = new Date().getFullYear();
  let totalWithYear = 0;
  let under20 = 0;

  for (const f of features) {
    const p = f?.properties ?? f;
    if (p.district !== district) continue;
    if (city && p.city !== city) continue;

    const raw = p.buildYear ?? p.buildingCompletionYear ?? null;
    if (raw === null) continue;

    let year: number | null = null;
    if (typeof raw === "number" && raw >= 100_000) {
      year = Math.floor(raw / 10_000) + 1911; // ROC YYMMDD
    } else if (typeof raw === "number" && raw < 200) {
      year = raw + 1911; // plain ROC year
    } else if (typeof raw === "number" && raw >= 1900 && raw <= 2200) {
      year = raw; // Western year
    }

    if (year === null || year < 1900 || year > refYear + 1) continue;
    const age = refYear - year;
    if (age < 0 || age >= 200) continue;

    totalWithYear++;
    if (age < 20) under20++;
  }

  if (totalWithYear < 3) return null; // not enough data
  return under20 / totalWithYear;
}

// ── Main scoring function ────────────────────────────────────────────────────

/**
 * Compute weighted composite scores for all districts.
 *
 * @param districts  Array of district data objects.
 * @param budgetWan  User's total purchase budget in 萬 (e.g. 1500 for 1500萬).
 * @param anchor     Commute anchor coordinates.
 * @param weights    Five dimension weights (each 0–10).
 * @param allFeatures All transaction features (for newness computation).
 * @returns Array sorted descending by composite score.
 */
export function computeWeightedScores(
  districts: DistrictForScoring[],
  budgetWan: number,
  anchor: { lat: number; lng: number },
  weights: PriorityWeights,
  allFeatures: any[] = [],
): WeightedDistrictScore[] {
  if (districts.length === 0) return [];

  // Step 1: Extract raw values per dimension
  const rawCost = districts.map((d) => rawCostValue(d.medianPriceWan));
  const rawCommute = districts.map((d) =>
    rawCommuteValue(d.lat, d.lng, anchor.lat, anchor.lng),
  );
  const rawSpace = districts.map((d) =>
    rawSpaceValue(d.medianPriceWan, budgetWan),
  );
  const rawNewness = districts.map((d) =>
    rawNewnessValue(allFeatures, d.district, d.city),
  );
  // Facilities is already 0–100 normalized by facilitiesUtils, use directly
  const rawFacilities = districts.map((d) => d.facilitiesScore);

  // Step 2: Normalize each dimension to 0–100 across the cohort
  const normCost = minMaxNormalize(rawCost, true);        // lower price → higher raw → higher score
  const normCommute = minMaxNormalize(rawCommute, true);  // already directional (0–100 decay score)
  const normSpace = minMaxNormalize(rawSpace, true);      // more 坪 → better
  const normNewness = minMaxNormalize(rawNewness, true);  // newer fraction → better
  // Facilities: already 0–100, but re-normalize within cohort for same scale
  const normFacilities = minMaxNormalize(rawFacilities, true);

  // Step 3: Compute weighted composites
  const totalWeight = weights.cost + weights.commute + weights.space + weights.newness + weights.facilities;

  const results: WeightedDistrictScore[] = districts.map((d, i) => {
    const scores: DimensionScores = {
      cost:       normCost[i] ?? null,
      commute:    normCommute[i] ?? null,
      space:      normSpace[i] ?? null,
      newness:    normNewness[i] ?? null,
      facilities: normFacilities[i] ?? null,
    };

    let composite = 0;
    if (totalWeight > 0) {
      let weightedSum = 0;
      let usedWeight = 0;
      const dims: [keyof DimensionScores, keyof PriorityWeights][] = [
        ["cost", "cost"],
        ["commute", "commute"],
        ["space", "space"],
        ["newness", "newness"],
        ["facilities", "facilities"],
      ];
      for (const [dim, wKey] of dims) {
        const s = scores[dim];
        const w = weights[wKey];
        if (s !== null && w > 0) {
          weightedSum += w * s;
          usedWeight += w;
        }
      }
      composite = usedWeight > 0 ? Math.round(weightedSum / usedWeight) : 0;
    }

    return {
      district: d.district,
      city: d.city,
      composite,
      dimensions: scores,
    };
  });

  // Step 4: Sort descending by composite
  return results.sort((a, b) => b.composite - a.composite);
}

// ── Label helpers ─────────────────────────────────────────────────────────────

/** Chinese label for each scoring dimension. */
export const DIMENSION_LABELS: Record<keyof PriorityWeights, string> = {
  cost: "負擔能力",
  commute: "通勤便利",
  space: "可購坪數",
  newness: "屋齡新穎",
  facilities: "生活機能",
};

/** Emoji for each scoring dimension. */
export const DIMENSION_EMOJIS: Record<keyof PriorityWeights, string> = {
  cost: "💰",
  commute: "🚉",
  space: "📐",
  newness: "🏗",
  facilities: "🏪",
};

/**
 * Format a 0–100 score into a colored HTML badge string (no DOM — returns safe HTML string).
 * High ≥ 70 → green, Mid ≥ 40 → yellow, Low → red.
 */
export function dimensionScoreBadgeClass(score: number | null): string {
  if (score === null) return "dim-na";
  if (score >= 70) return "dim-high";
  if (score >= 40) return "dim-mid";
  return "dim-low";
}
