import { describe, it, expect } from "vitest";
import {
  parseDateYearMonth,
  computeMonthlySeasonality,
  type SeasonalityResult,
  type SeasonalityTier,
} from "../lib/seasonalityUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  city: string,
  date: string,
  unitPrice: number,
): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, city, date, unitPrice },
  };
}

/**
 * Build N features spread across `yearCount` calendar years starting from `startYear`.
 * Each year gets one transaction per month (months determined by `monthsPerYear`).
 */
function makeMultiYearFeatures(
  district: string,
  city: string,
  startYear: number,
  yearCount: number,
  basePrice = 300000,
  monthsPerYear: number[] | null = null, // null → all 12 months
): any[] {
  const features: any[] = [];
  for (let y = 0; y < yearCount; y++) {
    const year = startYear + y;
    const months = monthsPerYear ?? [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    for (const mo of months) {
      features.push(makeFeature(
        district,
        city,
        `${year}-${String(mo).padStart(2, "0")}-15`,
        basePrice + y * 1000, // slight yearly drift
      ));
    }
  }
  return features;
}

// ── parseDateYearMonth ────────────────────────────────────────────────────────

describe("parseDateYearMonth", () => {
  it("parses YYYY-MM-DD", () => {
    expect(parseDateYearMonth("2024-03-15")).toEqual({ year: 2024, month: 3 });
  });

  it("parses YYYY/MM/DD", () => {
    expect(parseDateYearMonth("2023/07/01")).toEqual({ year: 2023, month: 7 });
  });

  it("parses single-digit month", () => {
    expect(parseDateYearMonth("2024-3-15")).toEqual({ year: 2024, month: 3 });
  });

  it("handles full-width CJK digits (NFKC normalisation)", () => {
    // Full-width digits: ２０２４－０３－１５
    expect(parseDateYearMonth("２０２４－０３－１５")).toEqual({ year: 2024, month: 3 });
  });

  it("handles mixed full-width and half-width", () => {
    expect(parseDateYearMonth("２０２３-07-01")).toEqual({ year: 2023, month: 7 });
  });

  it("returns null for null input", () => {
    expect(parseDateYearMonth(null)).toBeNull();
  });

  it("returns null for undefined input", () => {
    expect(parseDateYearMonth(undefined)).toBeNull();
  });

  it("returns null for numeric input", () => {
    expect(parseDateYearMonth(20240315)).toBeNull();
  });

  it("returns null for empty string", () => {
    expect(parseDateYearMonth("")).toBeNull();
  });

  it("returns null for malformed date string", () => {
    expect(parseDateYearMonth("not-a-date")).toBeNull();
  });

  it("returns null for 2-digit year", () => {
    expect(parseDateYearMonth("24-03-15")).toBeNull();
  });

  it("returns null for month > 12", () => {
    expect(parseDateYearMonth("2024-13-01")).toBeNull();
  });

  it("returns null for month 0", () => {
    expect(parseDateYearMonth("2024-00-01")).toBeNull();
  });

  it("parses YYYY-MM (no day) format", () => {
    expect(parseDateYearMonth("2024-05")).toEqual({ year: 2024, month: 5 });
  });
});

// ── computeMonthlySeasonality — guard conditions ──────────────────────────────

describe("computeMonthlySeasonality — guards", () => {
  it("returns sufficient=false for empty features array", () => {
    const result = computeMonthlySeasonality([], "大安區", "台北市");
    expect(result.sufficient).toBe(false);
  });

  it("returns sufficient=false when features is null-ish (empty array)", () => {
    const result = computeMonthlySeasonality([], "大安區");
    expect(result.sufficient).toBe(false);
    expect(result.volumeByMonth).toHaveLength(12);
    expect(result.priceIndexByMonth).toHaveLength(12);
  });

  it("returns sufficient=false for empty districtId", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3);
    const result = computeMonthlySeasonality(features, "");
    expect(result.sufficient).toBe(false);
  });

  it("returns sufficient=false for 1 distinct year of data", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2024, 1);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(false);
    expect(result.distinctYears).toBe(1);
  });

  it("returns sufficient=false for exactly 2 distinct years", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2023, 2);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(false);
    expect(result.distinctYears).toBe(2);
  });

  it("returns sufficient=true for exactly 3 distinct years", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2022, 3);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.distinctYears).toBe(3);
  });

  it("returns sufficient=false when 3 distinct years but < 12 (year, month) pairs", () => {
    // Only 1 month per year → 3 pairs, not enough
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000, [1]);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(false);
    expect(result.distinctYears).toBe(3);
    expect(result.minTxMonths).toBe(3);
  });

  it("filters by districtId — ignores other districts", () => {
    const features = [
      ...makeMultiYearFeatures("大安區", "台北市", 2021, 3),
      ...makeMultiYearFeatures("信義區", "台北市", 2021, 3),
    ];
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.distinctYears).toBe(3);
  });

  it("filters by city when provided", () => {
    const features = [
      ...makeMultiYearFeatures("中正區", "台北市", 2021, 3),
      ...makeMultiYearFeatures("中正區", "高雄市", 2021, 3),
    ];
    const taipei = computeMonthlySeasonality(features, "中正區", "台北市");
    const kaohsiung = computeMonthlySeasonality(features, "中正區", "高雄市");
    // Both should be sufficient
    expect(taipei.sufficient).toBe(true);
    expect(kaohsiung.sufficient).toBe(true);
    // District-only (no city filter) gets all features
    const all = computeMonthlySeasonality(features, "中正區");
    expect(all.sufficient).toBe(true);
  });

  it("skips features with missing district property", () => {
    const features = [
      { properties: { city: "台北市", date: "2021-03-15", unitPrice: 300000 } },
      ...makeMultiYearFeatures("大安區", "台北市", 2021, 3),
    ];
    const result = computeMonthlySeasonality(features, "大安區");
    expect(result.sufficient).toBe(true);
  });

  it("skips features with unparseable date", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3);
    features.push(makeFeature("大安區", "台北市", "bad-date", 500000));
    const result = computeMonthlySeasonality(features, "大安區");
    expect(result.sufficient).toBe(true); // 3 valid years remain
  });

  it("normalises full-width district names in features (NFKC)", () => {
    // Features use full-width district name (e.g. from a gov CSV field)
    // U+FF24U+FF41U+FF4EU+FF41U+FF41 = ｄａｎａａ — use real full-width CJK:
    // "大" in full-width: ｄ-style is not common; instead use full-width latin in district
    // Simulate: feature has "Ａ區" (full-width A = U+FF21), search with ASCII "A區"
    const fullWidthDistrict = "\uFF21\u533A"; // Ａ區
    const halfWidthDistrict = "A\u533A";      // A区 → NFKC → same
    const features = makeMultiYearFeatures(fullWidthDistrict, "台北市", 2021, 3, 300000);
    // querying with half-width should match via NFKC normalization
    const result = computeMonthlySeasonality(features, halfWidthDistrict, "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.distinctYears).toBe(3);
  });
});

// ── computeMonthlySeasonality — volume output ────────────────────────────────

describe("computeMonthlySeasonality — volumeByMonth", () => {
  it("returns 12-element volumeByMonth array", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.volumeByMonth).toHaveLength(12);
  });

  it("averages correctly when each month has equal counts across years", () => {
    // 3 years, 1 tx per month → average = 1.0 for each month
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    for (const avg of result.volumeByMonth) {
      expect(avg).toBeCloseTo(1.0, 5);
    }
  });

  it("sets missing months to 0 in volumeByMonth", () => {
    // Only 4 months of data across 3 years → 12 pairs, passes the guard
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000, [1, 4, 7, 10]);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.volumeByMonth[0]).toBeGreaterThan(0);  // January
    expect(result.volumeByMonth[3]).toBeGreaterThan(0);  // April
    expect(result.volumeByMonth[6]).toBeGreaterThan(0);  // July
    expect(result.volumeByMonth[9]).toBeGreaterThan(0);  // October
    // Other months should be 0
    for (let i = 0; i < 12; i++) {
      if (![0, 3, 6, 9].includes(i)) expect(result.volumeByMonth[i]).toBe(0);
    }
  });

  it("computes annualVolumeAvg as count-weighted mean", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3); // 36 yr-mo pairs, 1 tx each
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    // totalCount = 36, yrMoPairs = 36 → avg = 1.0
    expect(result.annualVolumeAvg).toBeCloseTo(1.0, 5);
    expect(result.minTxMonths).toBe(36);
  });
});

// ── computeMonthlySeasonality — price index ──────────────────────────────────

describe("computeMonthlySeasonality — priceIndexByMonth", () => {
  it("returns 12-element priceIndexByMonth array", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.priceIndexByMonth).toHaveLength(12);
  });

  it("returns index ≈ 100 when all months have equal prices", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    for (const idx of result.priceIndexByMonth) {
      if (idx !== null) expect(idx).toBeCloseTo(100.0, 0);
    }
  });

  it("returns null for months with no price data", () => {
    // Only 4 months of data across 3 years → 12 pairs, passes the guard
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000, [1, 2, 3, 4]);
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.priceIndexByMonth[0]).not.toBeNull(); // January
    expect(result.priceIndexByMonth[1]).not.toBeNull(); // February
    expect(result.priceIndexByMonth[2]).not.toBeNull(); // March
    expect(result.priceIndexByMonth[3]).not.toBeNull(); // April
    for (let i = 4; i < 12; i++) {
      expect(result.priceIndexByMonth[i]).toBeNull();
    }
  });

  it("reflects higher price months with index > 100", () => {
    // January price = 400000, all other months = 300000
    const features: any[] = [];
    for (let year = 2021; year <= 2023; year++) {
      features.push(makeFeature("大安區", "台北市", `${year}-01-15`, 400000));
      for (let mo = 2; mo <= 12; mo++) {
        features.push(makeFeature("大安區", "台北市", `${year}-${String(mo).padStart(2, "0")}-15`, 300000));
      }
    }
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    expect(result.priceIndexByMonth[0]).toBeGreaterThan(100); // January is pricier
    for (let i = 1; i < 12; i++) {
      expect(result.priceIndexByMonth[i]).toBeLessThan(100); // other months cheaper
    }
  });

  it("ignores invalid unitPrice values (zero or non-finite)", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000);
    // Add invalid entries
    features.push(makeFeature("大安區", "台北市", "2022-06-15", 0));
    features.push(makeFeature("大安區", "台北市", "2022-07-15", NaN));
    features.push(makeFeature("大安區", "台北市", "2022-08-15", -1));
    const result = computeMonthlySeasonality(features, "大安區", "台北市");
    // Should still compute correctly based on valid entries
    expect(result.sufficient).toBe(true);
  });
});

// ── computeMonthlySeasonality — currentMonthTier ─────────────────────────────

describe("computeMonthlySeasonality — currentMonthTier", () => {
  it("returns 'low' when current month index < 95 (淡季)", () => {
    // January much cheaper: price 200000 vs 300000 for others
    const features: any[] = [];
    for (let year = 2021; year <= 2023; year++) {
      features.push(makeFeature("大安區", "台北市", `${year}-01-15`, 200000));
      for (let mo = 2; mo <= 12; mo++) {
        features.push(makeFeature("大安區", "台北市", `${year}-${String(mo).padStart(2, "0")}-15`, 300000));
      }
    }
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 1); // nowMonth = January
    expect(result.currentMonthTier).toBe("low");
  });

  it("returns 'high' when current month index > 105 (旺季)", () => {
    // March much pricier: 500000 vs 300000
    const features: any[] = [];
    for (let year = 2021; year <= 2023; year++) {
      features.push(makeFeature("大安區", "台北市", `${year}-03-15`, 500000));
      for (let mo of [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12]) {
        features.push(makeFeature("大安區", "台北市", `${year}-${String(mo).padStart(2, "0")}-15`, 300000));
      }
    }
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 3); // nowMonth = March
    expect(result.currentMonthTier).toBe("high");
  });

  it("returns 'mid' when current month index is 95–105 (平季)", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000);
    // All prices equal → all indices ≈ 100 → mid
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 6);
    expect(result.currentMonthTier).toBe("mid");
  });

  it("returns null currentMonthTier when current month has no price data", () => {
    // Only months 1 and 2 have price data
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000, [1, 2]);
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 6); // June has no data
    expect(result.currentMonthTier).toBeNull();
  });

  it("returns sufficient=false and currentMonthTier=null for 2 years", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2023, 2);
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 3);
    expect(result.sufficient).toBe(false);
    expect(result.currentMonthTier).toBeNull();
  });

  it("accepts nowMonth=12 (December boundary)", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000);
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 12);
    expect(result.currentMonthTier).toBe("mid");
  });

  it("accepts nowMonth=1 (January boundary)", () => {
    const features = makeMultiYearFeatures("大安區", "台北市", 2021, 3, 300000);
    const result = computeMonthlySeasonality(features, "大安區", "台北市", 1);
    expect(result.currentMonthTier).toBe("mid");
  });
});
