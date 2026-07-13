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
 * Compute the raw "cost" (affordability) value:
 * **fraction of transactions in the district whose total price is ≤ budget**.
 *
 * This is genuinely distinct from the "space" dimension (which measures
 * median 坪 per 萬):
 * - cost captures the price *distribution* (how many units are in reach), not just the median.
 * - cost is strongly budget-dependent: raising the budget lifts more districts at different rates.
 * - space measures efficiency at the median price point.
 *
 * Returns null when district has < 3 transactions with valid total prices.
 */
export function rawCostValue(
  features: any[],
  district: string,
  city: string,
  budgetWan: number,
): number | null {
  if (!features || features.length === 0 || budgetWan <= 0) return null;

  const budgetNTD = budgetWan * 10_000; // convert 萬 → NT$
  let total = 0;
  let affordable = 0;

  for (const f of features) {
    const p = f?.properties ?? f;
    if (p.district !== district) continue;
    if (city && p.city !== city) continue;
    const totalPrice: number | null = typeof p.totalPrice === "number" && p.totalPrice > 0
      ? p.totalPrice
      : null;
    if (totalPrice === null) continue;
    total++;
    if (totalPrice <= budgetNTD) affordable++;
  }

  if (total < 3) return null; // insufficient data
  return affordable / total; // 0–1 fraction, higher = more transactions within budget
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
    if (p.city !== city) continue; // always filter by city to avoid cross-city same-name districts

    const raw = p.buildYear ?? p.buildingCompletionYear ?? null;
    if (raw === null) continue;

    let year: number | null = null;
    let n: number;

    if (typeof raw === "string") {
      const s = raw.normalize("NFKC").trim();
      if (!/^\d+$/.test(s)) continue;
      n = parseInt(s, 10);
    } else if (typeof raw === "number" && isFinite(raw) && Math.floor(raw) === raw) {
      n = raw;
    } else {
      continue;
    }

    if (n >= 10_000_000) {
      // 8-digit Gregorian YYYYMMDD (e.g. 20180512) — check BEFORE the 6-digit ROC branch
      year = Math.floor(n / 10_000);
    } else if (n >= 100_000) {
      year = Math.floor(n / 10_000) + 1911; // ROC YYMMDD or YYYMMDD
    } else if (n < 200) {
      year = n + 1911; // plain ROC year
    } else if (n >= 1900 && n <= 2200) {
      year = n; // Western year
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
  const rawCost = districts.map((d) =>
    rawCostValue(allFeatures, d.district, d.city, budgetWan),
  );
  const rawCommute = districts.map((d) =>
    rawCommuteValue(d.lat, d.lng, anchor.lat, anchor.lng),
  );
  const rawSpace = districts.map((d) =>
    rawSpaceValue(d.medianPriceWan, budgetWan),
  );
  const rawNewness = districts.map((d) =>
    rawNewnessValue(allFeatures, d.district, d.city),
  );
  // Facilities: pre-computed 0–100 by facilitiesUtils (absolute semantics preserved).
  // Do NOT re-normalize within the cohort — that destroys the absolute scale and
  // turns e.g. [80, 85, 90] into [0, 50, 100], making all districts appear equally spread.
  // Use the raw 0–100 values directly for the "why" breakdown and composite.
  const normFacilities: (number | null)[] = districts.map((d) => d.facilitiesScore);

  // Step 2: Normalize the other 4 dimensions to 0–100 across the cohort (min-max)
  const normCost     = minMaxNormalize(rawCost, true);     // more transactions within budget → better
  const normCommute  = minMaxNormalize(rawCommute, true);  // closer to anchor → better
  const normSpace    = minMaxNormalize(rawSpace, true);    // more 坪 for the money → better
  const normNewness  = minMaxNormalize(rawNewness, true);  // higher new-unit fraction → better


  // Determine which dimensions have ANY non-null data across the cohort.
  // Dimensions where ALL districts have null are excluded from the denominator
  // (no data → dimension is neutral, not a penalty).
  const hasDimData = {
    cost:       normCost.some(v => v !== null),
    commute:    normCommute.some(v => v !== null),
    space:      normSpace.some(v => v !== null),
    newness:    normNewness.some(v => v !== null),
    facilities: normFacilities.some(v => v !== null),
  };

  // Effective denominator: sum of weights for dimensions that have at least some cohort data
  const effectiveTotalWeight =
    (hasDimData.cost       ? weights.cost       : 0) +
    (hasDimData.commute    ? weights.commute     : 0) +
    (hasDimData.space      ? weights.space       : 0) +
    (hasDimData.newness    ? weights.newness     : 0) +
    (hasDimData.facilities ? weights.facilities  : 0);

  const results: WeightedDistrictScore[] = districts.map((d, i) => {
    const scores: DimensionScores = {
      cost:       normCost[i] ?? null,
      commute:    normCommute[i] ?? null,
      space:      normSpace[i] ?? null,
      newness:    normNewness[i] ?? null,
      facilities: normFacilities[i] ?? null,
    };

    let composite = 0;
    if (effectiveTotalWeight > 0) {
      let weightedSum = 0;
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
        // Include dimension if it has cohort data (weight > 0 AND some district has data).
        // Null for this specific district → treated as 0 (district penalized if peers have data).
        if (w > 0 && hasDimData[dim]) {
          weightedSum += w * (s ?? 0);
        }
      }
      composite = Math.round(weightedSum / effectiveTotalWeight);
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
