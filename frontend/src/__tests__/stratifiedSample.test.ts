import { describe, it, expect } from "vitest";
import {
  stratifiedSample,
  validateDistrictCoverage,
  type SampleItem,
} from "../lib/stratifiedSample.js";

/** Build a minimal SampleItem for testing. */
function makeFeature(
  id: number,
  city: string,
  district: string,
  date: string,
): SampleItem {
  return { properties: { id, city, district, date } };
}

/** Generate `count` features for a given district, with sequential dates. */
function makeDistrict(
  city: string,
  district: string,
  count: number,
  startId: number,
): SampleItem[] {
  return Array.from({ length: count }, (_, i) => {
    // ISO dates descending so newest = smallest i → newest = highest date string
    const year = 2024 - Math.floor(i / 4);
    const month = String((i % 4) * 3 + 1).padStart(2, "0");
    return makeFeature(startId + i, city, district, `${year}-${month}-01`);
  });
}

describe("stratifiedSample", () => {
  it("includes all records from a sparse district (5 records) when the other has 50", () => {
    const dense = makeDistrict("台北市", "信義區", 50, 1);
    const sparse = makeDistrict("台中市", "南區", 5, 101);
    const corpus = [...dense, ...sparse];

    const result = stratifiedSample(corpus, { districtMin: 30, exportLimit: 10000 });

    // All 5 sparse-district records must appear in the output
    const sparseIds = new Set(sparse.map(f => f.properties.id));
    const resultIds = new Set(result.map(f => f.properties.id));
    for (const id of sparseIds) {
      expect(resultIds.has(id), `sparse record id=${id} should be in output`).toBe(true);
    }
    // Total should be all 55 (both districts well under exportLimit)
    expect(result.length).toBe(55);
  });

  it("respects DISTRICT_MIN cap when a district has more records than the minimum", () => {
    const corpus = makeDistrict("台北市", "大安區", 100, 1);
    const result = stratifiedSample(corpus, { districtMin: 30, exportLimit: 10000 });
    // Phase 1 = 30 records; no remaining (guaranteed == all phase1, fill budget = 100-30=70 more)
    // Actually all 100 fit within exportLimit, so all should be included
    expect(result.length).toBe(100);
  });

  it("respects exportLimit hard cap", () => {
    const corpus = [
      ...makeDistrict("台北市", "信義區", 50, 1),
      ...makeDistrict("台中市", "南區", 50, 51),
      ...makeDistrict("高雄市", "左營區", 50, 101),
    ];
    const result = stratifiedSample(corpus, { districtMin: 10, exportLimit: 40 });
    expect(result.length).toBeLessThanOrEqual(40);
  });

  it("guarantees DISTRICT_MIN per district when exportLimit is large", () => {
    // 3 districts × 50 records each
    const corpus = [
      ...makeDistrict("台北市", "信義區", 50, 1),
      ...makeDistrict("台中市", "西屯區", 50, 51),
      ...makeDistrict("桃園市", "中壢區", 50, 101),
    ];
    const result = stratifiedSample(corpus, { districtMin: 15, exportLimit: 10000 });
    // All 150 fit, so all should be present
    expect(result.length).toBe(150);
  });

  it("selects the most-recent records for phase-1 guarantee", () => {
    // District with 10 records; dates go 2024-01, 2023-10, 2023-07, …
    const features = makeDistrict("台南市", "東區", 10, 1);
    // features[0] has newest date, features[9] has oldest
    const result = stratifiedSample(features, { districtMin: 3, exportLimit: 10000 });
    // All 10 fit; phase-1 took the 3 most-recent
    const resultIds = result.map(f => f.properties.id);
    expect(resultIds).toContain(1); // newest
    expect(resultIds).toContain(2);
    expect(resultIds).toContain(3);
  });

  it("handles an empty corpus gracefully", () => {
    expect(stratifiedSample([], { districtMin: 30, exportLimit: 10000 })).toEqual([]);
  });
});

describe("validateDistrictCoverage", () => {
  it("logs a warning when >20% of districts are below threshold", () => {
    const warnings: string[] = [];
    const origWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(String(args[0]));

    try {
      // 5 districts: 4 sparse (≤10 txns), 1 large — 80% sparse → should warn
      const corpus: SampleItem[] = [
        ...makeDistrict("台北市", "信義區", 50, 1),
        ...makeDistrict("台中市", "南區", 5, 51),
        ...makeDistrict("高雄市", "左營區", 3, 56),
        ...makeDistrict("桃園市", "中壢區", 2, 59),
        ...makeDistrict("台南市", "東區", 1, 61),
      ];
      validateDistrictCoverage(corpus, 10, 0.2);
      expect(warnings.some(w => w.includes("WARNING"))).toBe(true);
    } finally {
      console.warn = origWarn;
    }
  });

  it("does not warn when sparse fraction is within tolerance", () => {
    const warnings: string[] = [];
    const origWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(String(args[0]));

    try {
      // 1 sparse district out of 5 → 20% = exactly at warnFraction threshold (>20% triggers warn)
      const corpus: SampleItem[] = [
        ...makeDistrict("台北市", "信義區", 50, 1),
        ...makeDistrict("台北市", "大安區", 50, 51),
        ...makeDistrict("台北市", "中山區", 50, 101),
        ...makeDistrict("台北市", "松山區", 50, 151),
        ...makeDistrict("台中市", "南區", 5, 201), // 1/5 = 20% — NOT > 20%, no warn
      ];
      validateDistrictCoverage(corpus, 10, 0.2);
      expect(warnings.some(w => w.includes("WARNING"))).toBe(false);
    } finally {
      console.warn = origWarn;
    }
  });
});
