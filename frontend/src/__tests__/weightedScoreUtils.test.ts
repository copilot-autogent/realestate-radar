import { describe, it, expect } from "vitest";
import {
  minMaxNormalize,
  rawSpaceValue,
  rawCostValue,
  rawCommuteValue,
  rawNewnessValue,
  computeWeightedScores,
  DEFAULT_WEIGHTS,
  type DistrictForScoring,
  type PriorityWeights,
} from "../lib/weightedScoreUtils.js";

// ── minMaxNormalize ────────────────────────────────────────────────────────────

describe("minMaxNormalize", () => {
  it("maps min to 0 and max to 100 when higherIsBetter=true", () => {
    const result = minMaxNormalize([10, 20, 30], true);
    expect(result).toEqual([0, 50, 100]);
  });

  it("maps min to 100 and max to 0 when higherIsBetter=false", () => {
    const result = minMaxNormalize([10, 20, 30], false);
    expect(result).toEqual([100, 50, 0]);
  });

  it("returns 50 for all values when all are equal (flat dimension)", () => {
    const result = minMaxNormalize([5, 5, 5], true);
    expect(result).toEqual([50, 50, 50]);
  });

  it("preserves null values as null", () => {
    const result = minMaxNormalize([null, 10, 20, null], true);
    expect(result[0]).toBeNull();
    expect(result[3]).toBeNull();
    expect(result[1]).toBe(0);
    expect(result[2]).toBe(100);
  });

  it("returns all null when all values are null", () => {
    const result = minMaxNormalize([null, null]);
    expect(result).toEqual([null, null]);
  });

  it("handles single non-null value (flat after null exclusion)", () => {
    const result = minMaxNormalize([null, 42, null]);
    expect(result[1]).toBe(50); // single value → flat → 50
  });
});

// ── rawSpaceValue ──────────────────────────────────────────────────────────────

describe("rawSpaceValue", () => {
  it("returns budget / price for valid inputs", () => {
    expect(rawSpaceValue(30, 1500)).toBeCloseTo(50); // 1500 / 30 = 50 坪
  });

  it("returns null when medianPrice is null", () => {
    expect(rawSpaceValue(null, 1500)).toBeNull();
  });

  it("returns null when budget is 0", () => {
    expect(rawSpaceValue(30, 0)).toBeNull();
  });

  it("returns null when price is 0", () => {
    expect(rawSpaceValue(0, 1500)).toBeNull();
  });
});

// ── rawCostValue ───────────────────────────────────────────────────────────────

describe("rawCostValue", () => {
  const refYear = new Date().getFullYear();
  const makeFeature = (district: string, city: string, totalPrice: number) => ({
    properties: { district, city, totalPrice },
  });

  it("returns fraction of transactions within budget (all affordable)", () => {
    const features = [
      makeFeature("大安區", "台北市", 8_000_000),
      makeFeature("大安區", "台北市", 9_000_000),
      makeFeature("大安區", "台北市", 10_000_000),
    ];
    // budget = 1000萬 = 10,000,000 NT$
    expect(rawCostValue(features, "大安區", "台北市", 1000)).toBe(1);
  });

  it("returns partial fraction when some transactions exceed budget", () => {
    const features = [
      makeFeature("大安區", "台北市", 8_000_000),
      makeFeature("大安區", "台北市", 12_000_000),
      makeFeature("大安區", "台北市", 15_000_000),
    ];
    // budget = 1000萬 = 10,000,000: only first transaction affordable
    expect(rawCostValue(features, "大安區", "台北市", 1000)).toBeCloseTo(1/3);
  });

  it("returns null when fewer than 3 transactions", () => {
    const features = [
      makeFeature("大安區", "台北市", 5_000_000),
      makeFeature("大安區", "台北市", 6_000_000),
    ];
    expect(rawCostValue(features, "大安區", "台北市", 1000)).toBeNull();
  });

  it("returns null for empty features", () => {
    expect(rawCostValue([], "大安區", "台北市", 1000)).toBeNull();
  });

  it("returns null when budget is 0", () => {
    const features = [
      makeFeature("大安區", "台北市", 5_000_000),
      makeFeature("大安區", "台北市", 6_000_000),
      makeFeature("大安區", "台北市", 7_000_000),
    ];
    expect(rawCostValue(features, "大安區", "台北市", 0)).toBeNull();
  });

  it("higher budget means more transactions affordable", () => {
    const features = [
      makeFeature("板橋區", "新北市", 5_000_000),
      makeFeature("板橋區", "新北市", 8_000_000),
      makeFeature("板橋區", "新北市", 12_000_000),
    ];
    const lowBudget = rawCostValue(features, "板橋區", "新北市", 700)!;
    const highBudget = rawCostValue(features, "板橋區", "新北市", 1500)!;
    expect(highBudget).toBeGreaterThan(lowBudget);
  });
});

// ── rawCommuteValue ────────────────────────────────────────────────────────────

describe("rawCommuteValue", () => {
  it("returns 100 when district is exactly at anchor", () => {
    expect(rawCommuteValue(25.047, 121.517, 25.047, 121.517)).toBe(100);
  });

  it("returns approximately 50 at ~8 km from anchor (half-life)", () => {
    // At lat 25°N, 1° longitude ≈ 100.9 km, so 8 km ≈ 0.079° east of anchor
    const score = rawCommuteValue(25.0478, 121.596, 25.0478, 121.517);
    expect(score).toBeGreaterThan(40);
    expect(score).toBeLessThan(65);
  });

  it("returns null when district centroid is at (0, 0)", () => {
    expect(rawCommuteValue(0, 0, 25.047, 121.517)).toBeNull();
  });

  it("returns lower score for farther district", () => {
    const close = rawCommuteValue(25.05, 121.52, 25.047, 121.517)!;
    const far = rawCommuteValue(24.95, 121.31, 25.047, 121.517)!;
    expect(close).toBeGreaterThan(far);
  });
});

// ── rawNewnessValue ────────────────────────────────────────────────────────────

describe("rawNewnessValue", () => {
  const refYear = new Date().getFullYear();
  const makeFeature = (district: string, city: string, buildYear: number) => ({
    properties: { district, city, buildYear },
  });

  it("returns fraction of transactions under 20 years old", () => {
    const features = [
      makeFeature("大安區", "台北市", refYear - 5),   // new
      makeFeature("大安區", "台北市", refYear - 10),  // new
      makeFeature("大安區", "台北市", refYear - 25),  // old
    ];
    const result = rawNewnessValue(features, "大安區", "台北市");
    expect(result).toBeCloseTo(2 / 3);
  });

  it("returns null for empty features array", () => {
    expect(rawNewnessValue([], "大安區", "台北市")).toBeNull();
  });

  it("returns null when fewer than 3 transactions have build year data", () => {
    const features = [
      makeFeature("大安區", "台北市", refYear - 5),
      makeFeature("大安區", "台北市", refYear - 10),
    ];
    expect(rawNewnessValue(features, "大安區", "台北市")).toBeNull();
  });

  it("filters by city to avoid cross-city same-name districts", () => {
    const features = [
      makeFeature("中山區", "台北市", refYear - 5),
      makeFeature("中山區", "台北市", refYear - 5),
      makeFeature("中山區", "台北市", refYear - 5),
      makeFeature("中山區", "基隆市", refYear - 40),
    ];
    const result = rawNewnessValue(features, "中山區", "台北市");
    expect(result).toBe(1); // only Taipei ones counted, all new
  });
});

// ── computeWeightedScores ──────────────────────────────────────────────────────

describe("computeWeightedScores", () => {
  const anchor = { lat: 25.0478, lng: 121.517 }; // Taipei Main

  const makeDistrict = (
    district: string,
    city: string,
    lat: number,
    lng: number,
    medianPriceWan: number | null,
    facilitiesScore: number | null,
  ): DistrictForScoring => ({
    district,
    city,
    medianPriceWan,
    lat,
    lng,
    facilitiesScore,
  });

  const districts: DistrictForScoring[] = [
    makeDistrict("大安區", "台北市", 25.026, 121.543, 90, 95),   // expensive + great facilities
    makeDistrict("桃園區", "桃園市", 24.989, 121.314, 20, 40),   // cheap + far
    makeDistrict("板橋區", "新北市", 25.014, 121.463, 40, 70),   // mid + close-ish
  ];

  it("returns one result per district", () => {
    const results = computeWeightedScores(districts, 1500, anchor, DEFAULT_WEIGHTS);
    expect(results).toHaveLength(3);
  });

  it("results are sorted descending by composite score", () => {
    const results = computeWeightedScores(districts, 1500, anchor, DEFAULT_WEIGHTS);
    for (let i = 0; i < results.length - 1; i++) {
      expect(results[i]!.composite).toBeGreaterThanOrEqual(results[i + 1]!.composite);
    }
  });

  it("composite is between 0 and 100", () => {
    const results = computeWeightedScores(districts, 1500, anchor, DEFAULT_WEIGHTS);
    for (const r of results) {
      expect(r.composite).toBeGreaterThanOrEqual(0);
      expect(r.composite).toBeLessThanOrEqual(100);
    }
  });

  it("setting cost weight to 10 and others to 0 ranks cheapest district first (most transactions within budget)", () => {
    // Build features: 桃園區 has lower prices → more within budget
    const features = [
      // 桃園區 - cheap
      ...Array(5).fill(null).map(() => ({ properties: { district: "桃園區", city: "桃園市", totalPrice: 5_000_000 } })),
      // 大安區 - expensive
      ...Array(5).fill(null).map(() => ({ properties: { district: "大安區", city: "台北市", totalPrice: 30_000_000 } })),
      // 板橋區 - mid
      ...Array(5).fill(null).map(() => ({ properties: { district: "板橋區", city: "新北市", totalPrice: 12_000_000 } })),
    ];
    const costOnly: PriorityWeights = { cost: 10, commute: 0, space: 0, newness: 0, facilities: 0 };
    const results = computeWeightedScores(districts, 1500, anchor, costOnly, features);
    // 桃園區 has all 5 transactions within 1500萬, should rank first for cost
    expect(results[0]!.district).toBe("桃園區");
  });

  it("setting facilities weight to 10 and others to 0 ranks 大安區 first", () => {
    const facOnly: PriorityWeights = { cost: 0, commute: 0, space: 0, newness: 0, facilities: 10 };
    const results = computeWeightedScores(districts, 1500, anchor, facOnly);
    // 大安區 has highest facilitiesScore (95)
    expect(results[0]!.district).toBe("大安區");
  });

  it("returns empty array for empty input", () => {
    expect(computeWeightedScores([], 1500, anchor, DEFAULT_WEIGHTS)).toEqual([]);
  });

  it("dimension scores are 0–100 or null", () => {
    const results = computeWeightedScores(districts, 1500, anchor, DEFAULT_WEIGHTS);
    for (const r of results) {
      const dims = Object.values(r.dimensions);
      for (const v of dims) {
        if (v !== null) {
          expect(v).toBeGreaterThanOrEqual(0);
          expect(v).toBeLessThanOrEqual(100);
        }
      }
    }
  });

  it("all-zero weights produce composite of 0 for all districts", () => {
    const zero: PriorityWeights = { cost: 0, commute: 0, space: 0, newness: 0, facilities: 0 };
    const results = computeWeightedScores(districts, 1500, anchor, zero);
    for (const r of results) {
      expect(r.composite).toBe(0);
    }
  });

  it("handles null medianPriceWan (space dimension is null)", () => {
    const d = [makeDistrict("測試區", "台北市", 25.026, 121.543, null, 50)];
    const results = computeWeightedScores(d, 1500, anchor, DEFAULT_WEIGHTS);
    expect(results[0]!.dimensions.space).toBeNull();
  });
});
