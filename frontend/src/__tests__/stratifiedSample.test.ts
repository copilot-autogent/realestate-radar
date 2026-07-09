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
    // Phase 1 takes 30 most-recent; Phase 2 fills remaining 70 — all 100 fit within exportLimit.
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

  it("enforces exportLimit even when phase1 alone exceeds it", () => {
    // 10 districts × 50 records → phase1 = 10×30 = 300 > exportLimit=50
    const corpus: SampleItem[] = [];
    for (let d = 0; d < 10; d++) {
      corpus.push(...makeDistrict("台北市", `district${d}`, 50, d * 50 + 1));
    }
    const result = stratifiedSample(corpus, { districtMin: 30, exportLimit: 50 });
    expect(result.length).toBe(50);
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
    // Build 10 features with strictly decreasing dates so newest = id 1001
    const features: SampleItem[] = Array.from({ length: 10 }, (_, i) => ({
      properties: {
        id: 1001 + i,
        city: "台南市",
        district: "東區",
        // 2024-10, 2024-09, …, 2024-01  (strictly newest-first)
        date: `2024-${String(10 - i).padStart(2, "0")}-01`,
      },
    }));
    // exportLimit=3 forces phase1 to be trimmed to exactly 3 records
    const result = stratifiedSample(features, { districtMin: 10, exportLimit: 3 });
    expect(result.length).toBe(3);
    // Must be the 3 with the newest dates: ids 1001, 1002, 1003
    const resultIds = result.map(f => f.properties.id).sort((a, b) => a - b);
    expect(resultIds).toEqual([1001, 1002, 1003]);
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
