import { describe, it, expect } from "vitest";
import {
  parseBuildYear,
  buildYearToAge,
  computeBuildingAgeDistribution,
  type AgeBucket,
  type BuildingAgeResult,
} from "../lib/buildingAgeUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Build a minimal GeoJSON feature for testing */
function makeFeature(district: string, city: string, buildYear: number | string | null): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, city, buildYear },
  };
}

/** Bulk-create N features for a district with the given age (relative to referenceYear) */
function makeFeaturesByAge(
  district: string,
  city: string,
  age: number,
  count: number,
  referenceYear: number,
): any[] {
  return Array.from({ length: count }, () =>
    makeFeature(district, city, referenceYear - age),
  );
}

const REF_YEAR = 2026;
const DISTRICT = "大安區";
const CITY = "台北市";

// ── parseBuildYear tests ──────────────────────────────────────────────────────

describe("parseBuildYear", () => {
  it("returns null for null input", () => {
    expect(parseBuildYear(null)).toBe(null);
  });

  it("returns null for undefined", () => {
    expect(parseBuildYear(undefined)).toBe(null);
  });

  it("returns null for empty string", () => {
    expect(parseBuildYear("")).toBe(null);
  });

  it("returns null for zero", () => {
    expect(parseBuildYear(0)).toBe(null);
  });

  it("parses plain ROC year (< 200) → Western year", () => {
    expect(parseBuildYear(74)).toBe(1985);
    expect(parseBuildYear(100)).toBe(2011);
    expect(parseBuildYear(113)).toBe(2024);
  });

  it("parses 4-digit Western year directly", () => {
    expect(parseBuildYear(1985)).toBe(1985);
    expect(parseBuildYear(2010)).toBe(2010);
    expect(parseBuildYear(2026)).toBe(2026);
  });

  it("parses 6-digit YYMMDD ROC date integer", () => {
    expect(parseBuildYear(741024)).toBe(1985); // ROC 74/10/24
    expect(parseBuildYear(860722)).toBe(1997); // ROC 86/07/22
    expect(parseBuildYear(670301)).toBe(1978); // ROC 67/03/01
  });

  it("parses 7-digit YYYMMDD ROC date integer", () => {
    expect(parseBuildYear(1081128)).toBe(2019); // ROC 108/11/28
    expect(parseBuildYear(1150507)).toBe(2026); // ROC 115/05/07
  });

  it("NFKC: parses full-width digit string (U+FF10–FF19)", () => {
    // ７４ = "\uFF17\uFF14" in full-width
    expect(parseBuildYear("７４")).toBe(1985);
  });

  it("NFKC: parses full-width 6-digit ROC date string", () => {
    // ７４１０２４ = full-width "741024"
    expect(parseBuildYear("７４１０２４")).toBe(1985);
  });

  it("parses ASCII string representation of a 4-digit Western year", () => {
    expect(parseBuildYear("1985")).toBe(1985);
    expect(parseBuildYear("2010")).toBe(2010);
  });

  it("returns null for strings with non-numeric content", () => {
    expect(parseBuildYear("abc")).toBe(null);
    expect(parseBuildYear("不明")).toBe(null);
  });

  it("returns null for implausibly old Western year (< 1900)", () => {
    expect(parseBuildYear(1800)).toBe(null);
  });

  it("returns null for negative numbers", () => {
    expect(parseBuildYear(-1)).toBe(null);
  });
});

// ── buildYearToAge tests ──────────────────────────────────────────────────────

describe("buildYearToAge", () => {
  it("computes age from valid build year", () => {
    expect(buildYearToAge(2020, 2026)).toBe(6);
    expect(buildYearToAge(1985, 2026)).toBe(41);
    expect(buildYearToAge(2026, 2026)).toBe(0);
  });

  it("returns null for null buildYear", () => {
    expect(buildYearToAge(null, 2026)).toBe(null);
  });

  it("returns null when age is negative (build year in future)", () => {
    expect(buildYearToAge(2030, 2026)).toBe(null);
  });

  it("returns null for implausible age >= 200", () => {
    expect(buildYearToAge(1800, 2026)).toBe(null);
  });
});

// ── computeBuildingAgeDistribution tests ─────────────────────────────────────

describe("computeBuildingAgeDistribution", () => {
  it("returns sufficient=false when fewer than 5 transactions have year data", () => {
    const features = [
      makeFeature(DISTRICT, CITY, 1990),
      makeFeature(DISTRICT, CITY, 1995),
      makeFeature(DISTRICT, CITY, null),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.sufficient).toBe(false);
  });

  it("returns sufficient=true when ≥ 5 transactions have year data", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 10, 6, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.sufficient).toBe(true);
  });

  it("bucket boundary: age=0 goes into < 5年 bucket", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 0, 5, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[0]!.count).toBe(5); // < 5年
  });

  it("bucket boundary: age=4 goes into < 5年, age=5 goes into 5–10年", () => {
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 4, 1, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 5, 4, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[0]!.count).toBe(1); // < 5年
    expect(result.buckets[1]!.count).toBe(4); // 5–10年
  });

  it("bucket boundary: age=10 goes into 10–20年 (not 5–10年)", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 10, 5, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[1]!.count).toBe(0); // 5–10年
    expect(result.buckets[2]!.count).toBe(5); // 10–20年
  });

  it("bucket boundary: age=20 goes into 20–30年, age=30 into 30–40年", () => {
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 20, 3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 30, 3, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[3]!.count).toBe(3); // 20–30年
    expect(result.buckets[4]!.count).toBe(3); // 30–40年
  });

  it("bucket boundary: age=40 goes into > 40年", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 40, 5, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[5]!.count).toBe(5); // > 40年
  });

  it("pct sums to 100 (rounding may cause ±1 deviation but individual values correct)", () => {
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 2,  2, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 7,  2, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 15, 2, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 25, 2, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 35, 1, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 45, 1, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets.every((b) => b.pct >= 0 && b.pct <= 100)).toBe(true);
    // Each pct = Math.round(2/10*100) = 20 for the 2-count buckets
    expect(result.buckets[0]!.pct).toBe(20);
    expect(result.buckets[2]!.pct).toBe(20);
  });

  it("completeness: 50% when half have null buildYear", () => {
    const withYear = makeFeaturesByAge(DISTRICT, CITY, 10, 5, REF_YEAR);
    const withoutYear = Array.from({ length: 5 }, () => makeFeature(DISTRICT, CITY, null));
    const result = computeBuildingAgeDistribution(
      [...withYear, ...withoutYear],
      DISTRICT, CITY, REF_YEAR,
    );
    expect(result.completenessPercent).toBe(50);
  });

  it("completeness: 100% when all have valid buildYear", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 15, 8, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.completenessPercent).toBe(100);
  });

  it("completeness: 0% when no features", () => {
    const result = computeBuildingAgeDistribution([], DISTRICT, CITY, REF_YEAR);
    expect(result.completenessPercent).toBe(0);
    expect(result.sufficient).toBe(false);
  });

  it("NFKC: features with full-width buildYear strings are parsed correctly", () => {
    // "７４１０２４" = 741024 in full-width → ROC 74/10/24 → 1985 → age 2026-1985=41
    const features = Array.from({ length: 5 }, () =>
      makeFeature(DISTRICT, CITY, "７４１０２４"),
    );
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.sufficient).toBe(true);
    expect(result.buckets[5]!.count).toBe(5); // all in > 40年
  });

  it("NFKC: 6-digit ROC integer (741024) is parsed correctly → age 41 → > 40年 bucket", () => {
    const features = Array.from({ length: 5 }, () =>
      makeFeature(DISTRICT, CITY, 741024),
    );
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets[5]!.count).toBe(5); // > 40年
  });

  it("annotation: 都更風險 when > 40% in >30年 buckets", () => {
    // 3 in 30-40年, 3 in >40年 (6/10 = 60% > 40%)
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 35, 3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 45, 3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 10, 4, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.annotation).toBe("urban-renewal-risk");
  });

  it("annotation: NO 都更風險 when exactly 40% in >30年 (threshold is strict >)", () => {
    // 4 in >30年, 6 in other (4/10 = 40% — NOT > 40%)
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 35, 4, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 10, 6, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.annotation).not.toBe("urban-renewal-risk");
  });

  it("annotation: 新屋為主 when > 50% in <10年 buckets", () => {
    // 3 in <5年, 3 in 5-10年 (6/10 = 60% > 50%)
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 2,  3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 7,  3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 20, 4, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.annotation).toBe("mostly-new");
  });

  it("annotation: NO 新屋為主 when exactly 50% in <10年 (threshold is strict >)", () => {
    // 5 in <10年, 5 in older (5/10 = 50% — NOT > 50%)
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 3,  5, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 20, 5, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.annotation).not.toBe("mostly-new");
  });

  it("annotation: null when no threshold met", () => {
    const features = [
      ...makeFeaturesByAge(DISTRICT, CITY, 2,  2, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 15, 3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 25, 3, REF_YEAR),
      ...makeFeaturesByAge(DISTRICT, CITY, 35, 2, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.annotation).toBe(null);
  });

  it("annotation: null when sufficient=false", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 45, 3, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.sufficient).toBe(false);
    expect(result.annotation).toBe(null);
  });

  it("filters by city when provided", () => {
    const features = [
      ...makeFeaturesByAge("中山區", "台北市", 10, 5, REF_YEAR),
      ...makeFeaturesByAge("中山區", "基隆市", 45, 5, REF_YEAR),
    ];
    const result = computeBuildingAgeDistribution(features, "中山區", "台北市", REF_YEAR);
    // Only 台北市 features should count
    expect(result.buckets[2]!.count).toBe(5); // 10–20年
    expect(result.buckets[5]!.count).toBe(0); // > 40年 (基隆市 excluded)
  });

  it("bucket labels match spec", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 10, 5, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    expect(result.buckets.map((b) => b.label)).toEqual([
      "< 5年", "5–10年", "10–20年", "20–30年", "30–40年", "> 40年",
    ]);
  });

  it("filterValue mapping is correct for all buckets", () => {
    const features = makeFeaturesByAge(DISTRICT, CITY, 10, 5, REF_YEAR);
    const result = computeBuildingAgeDistribution(features, DISTRICT, CITY, REF_YEAR);
    const filterValues = result.buckets.map((b) => b.filterValue);
    expect(filterValues).toEqual(["0-5", "5-20", "5-20", "20-40", "20-40", "40+"]);
  });
});
