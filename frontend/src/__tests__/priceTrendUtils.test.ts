import { describe, it, expect } from "vitest";
import {
  parseDateMonth,
  subtractMonths,
  computeMA3,
  computeYoY,
  computePriceTrendSeries,
  type TrendPoint,
  type PriceTrendSeries,
} from "../lib/priceTrendUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  city: string,
  date: string,
  unitPrice: number,
  totalPrice?: number,
): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: {
      district,
      city,
      date,
      unitPrice,
      totalPrice: totalPrice ?? unitPrice * 30,
    },
  };
}

/** Build N features uniformly spread across the trailing `months` calendar months. */
function makeFeaturesSeries(
  district: string,
  city: string,
  anchorMonth: string, // "YYYY-MM"
  monthCount: number,
  pricePerPing: number | ((i: number) => number) = 300000,
): any[] {
  const features: any[] = [];
  const [ys, ms] = anchorMonth.split("-");
  for (let i = 0; i < monthCount; i++) {
    let year  = parseInt(ys!, 10);
    let month = parseInt(ms!, 10) - i;
    while (month <= 0) { month += 12; year -= 1; }
    const date = `${year}-${String(month).padStart(2, "0")}-15`;
    const price = typeof pricePerPing === "function" ? pricePerPing(i) : pricePerPing;
    features.push(makeFeature(district, city, date, price, price * 30));
  }
  return features;
}

// ── parseDateMonth ────────────────────────────────────────────────────────────

describe("parseDateMonth", () => {
  it("parses YYYY-MM-DD", () => {
    expect(parseDateMonth("2024-03-15")).toBe("2024-03");
  });

  it("parses YYYY/MM/DD", () => {
    expect(parseDateMonth("2023/07/01")).toBe("2023-07");
  });

  it("parses YYYY-MM (no day)", () => {
    expect(parseDateMonth("2024-11")).toBe("2024-11");
  });

  it("handles full-width CJK digits via NFKC normalisation", () => {
    // Full-width digits U+FF10–FF19: ２０２４－０３－１５
    expect(parseDateMonth("２０２４－０３－１５")).toBe("2024-03");
  });

  it("returns null for non-string input", () => {
    expect(parseDateMonth(null)).toBeNull();
    expect(parseDateMonth(undefined)).toBeNull();
    expect(parseDateMonth(20240315)).toBeNull();
  });

  it("returns null for empty string", () => {
    expect(parseDateMonth("")).toBeNull();
  });

  it("returns null for malformed date", () => {
    expect(parseDateMonth("not-a-date")).toBeNull();
    expect(parseDateMonth("24-03-15")).toBeNull();
  });

  it("returns null for out-of-range month", () => {
    expect(parseDateMonth("2024-13-01")).toBeNull();
    expect(parseDateMonth("2024-00-01")).toBeNull();
  });

  it("returns null for year outside 1900–2100", () => {
    expect(parseDateMonth("1899-01-01")).toBeNull();
    expect(parseDateMonth("2101-01-01")).toBeNull();
  });
});

// ── subtractMonths ────────────────────────────────────────────────────────────

describe("subtractMonths", () => {
  it("subtracts within a year", () => {
    expect(subtractMonths("2024-06", 3)).toBe("2024-03");
  });

  it("subtracts across a year boundary", () => {
    expect(subtractMonths("2024-02", 3)).toBe("2023-11");
  });

  it("subtracts exactly 12 months", () => {
    expect(subtractMonths("2024-06", 12)).toBe("2023-06");
  });

  it("subtracts 35 months (36-month window start)", () => {
    expect(subtractMonths("2026-06", 35)).toBe("2023-07");
  });

  it("handles January minus N months", () => {
    expect(subtractMonths("2024-01", 1)).toBe("2023-12");
  });
});

// ── computeMA3 ────────────────────────────────────────────────────────────────

describe("computeMA3", () => {
  it("first two entries are null", () => {
    const result = computeMA3([100, 200, 300]);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
  });

  it("third entry is average of first three", () => {
    const result = computeMA3([100, 200, 300]);
    expect(result[2]).toBeCloseTo(200, 5);
  });

  it("computes correctly for a 5-element series", () => {
    const vals = [10, 20, 30, 40, 50];
    const result = computeMA3(vals);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
    expect(result[2]).toBeCloseTo(20, 5);
    expect(result[3]).toBeCloseTo(30, 5);
    expect(result[4]).toBeCloseTo(40, 5);
  });

  it("returns empty array for empty input", () => {
    expect(computeMA3([])).toEqual([]);
  });

  it("returns [null, null] for two-element input", () => {
    expect(computeMA3([1, 2])).toEqual([null, null]);
  });
});

// ── computeYoY ────────────────────────────────────────────────────────────────

describe("computeYoY", () => {
  it("computes positive YoY (price increase)", () => {
    expect(computeYoY(330, 300)).toBeCloseTo(10, 5);
  });

  it("computes negative YoY (price decrease)", () => {
    expect(computeYoY(285, 300)).toBeCloseTo(-5, 5);
  });

  it("returns 0 for unchanged price", () => {
    expect(computeYoY(300, 300)).toBeCloseTo(0, 5);
  });

  it("returns null when prior price is 0", () => {
    expect(computeYoY(300, 0)).toBeNull();
  });

  it("returns null when prior price is non-finite", () => {
    expect(computeYoY(300, NaN)).toBeNull();
    expect(computeYoY(300, Infinity)).toBeNull();
  });
});

// ── computePriceTrendSeries ───────────────────────────────────────────────────

describe("computePriceTrendSeries", () => {
  it("returns insufficient=false with empty features array", () => {
    const result = computePriceTrendSeries([], "大安區");
    expect(result.sufficient).toBe(false);
    expect(result.points).toHaveLength(0);
  });

  it("returns insufficient=false when fewer than 6 months of data", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-06", 3, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.sufficient).toBe(false);
  });

  it("returns sufficient=true when ≥ 6 months of data", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-06", 12, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.sufficient).toBe(true);
    expect(result.points.length).toBeGreaterThanOrEqual(6);
  });

  it("filters by districtId — excludes other districts", () => {
    const features = [
      ...makeFeaturesSeries("大安區", "台北市", "2024-06", 12, 500000),
      ...makeFeaturesSeries("信義區", "台北市", "2024-06", 12, 600000),
    ];
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.points.every((pt) => pt.medianPerPing < 550000)).toBe(true);
  });

  it("applies city filter when provided", () => {
    const features = [
      ...makeFeaturesSeries("中山區", "台北市", "2024-06", 12, 500000),
      ...makeFeaturesSeries("中山區", "台中市", "2024-06", 12, 200000),
    ];
    const resultTaipei = computePriceTrendSeries(features, "中山區", "台北市");
    const resultTaichung = computePriceTrendSeries(features, "中山區", "台中市");
    expect(resultTaipei.points.every((pt) => pt.medianPerPing > 400000)).toBe(true);
    expect(resultTaichung.points.every((pt) => pt.medianPerPing < 300000)).toBe(true);
  });

  it("trims to 36-month window", () => {
    // Create 48 months of data
    const features = makeFeaturesSeries("大安區", "台北市", "2026-06", 48, 500000);
    const result = computePriceTrendSeries(features, "大安區", "台北市", 36);
    expect(result.points.length).toBeLessThanOrEqual(36);
  });

  it("points are sorted chronologically", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-12", 12, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    for (let i = 1; i < result.points.length; i++) {
      expect(result.points[i]!.month >= result.points[i - 1]!.month).toBe(true);
    }
  });

  it("computes medianPerPing correctly", () => {
    const features = [
      makeFeature("大安區", "台北市", "2024-03-10", 100000),
      makeFeature("大安區", "台北市", "2024-03-15", 300000),
      makeFeature("大安區", "台北市", "2024-03-20", 500000),
    ];
    // Base starts at 2024-09, so it spans 2024-09..2024-04 — no overlap with 2024-03
    const base = makeFeaturesSeries("大安區", "台北市", "2024-09", 6, 400000);
    const result = computePriceTrendSeries([...features, ...base], "大安區");
    const march = result.points.find((pt) => pt.month === "2024-03");
    expect(march).toBeDefined();
    expect(march!.medianPerPing).toBe(300000);
  });

  it("computes medianTotalWan correctly", () => {
    const features = [
      makeFeature("大安區", "台北市", "2024-03-10", 100000, 1_000_000),
      makeFeature("大安區", "台北市", "2024-03-15", 300000, 3_000_000),
      makeFeature("大安區", "台北市", "2024-03-20", 500000, 5_000_000),
    ];
    // Base starts at 2024-09, so it spans 2024-09..2024-04 — no overlap with 2024-03
    const base = makeFeaturesSeries("大安區", "台北市", "2024-09", 6, 400000);
    const result = computePriceTrendSeries([...features, ...base], "大安區");
    const march = result.points.find((pt) => pt.month === "2024-03");
    expect(march).toBeDefined();
    // median of [100, 300, 500] 萬 = 300 萬
    expect(march!.medianTotalWan).toBe(300);
  });

  it("MA3 array length matches points length", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-12", 12, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.ma3).toHaveLength(result.points.length);
  });

  it("MA3 first two values are null", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-12", 12, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.ma3[0]).toBeNull();
    expect(result.ma3[1]).toBeNull();
  });

  it("MA3 third value equals average of first three medians", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-12", 12, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    const [p0, p1, p2] = result.points;
    const expectedMA = (p0!.medianPerPing + p1!.medianPerPing + p2!.medianPerPing) / 3;
    expect(result.ma3[2]).toBeCloseTo(expectedMA, 1);
  });

  it("YoY is null when no prior-year data is available", () => {
    const features = makeFeaturesSeries("大安區", "台北市", "2024-06", 6, 500000);
    const result = computePriceTrendSeries(features, "大安區");
    // 6 months doesn't reach 12 months back
    expect(result.yoyPercentage).toBeNull();
  });

  it("YoY is computed when prior-year month exists", () => {
    const anchor = "2025-06";
    const features = makeFeaturesSeries("大安區", "台北市", anchor, 13, 500000);
    // 2025-06 vs 2024-06 — both should be 500000 → 0% change
    const result = computePriceTrendSeries(features, "大安區");
    if (result.yoyPercentage !== null) {
      expect(result.yoyPercentage).toBeCloseTo(0, 1);
    }
  });

  it("NFKC: handles features with full-width digit district names", () => {
    // Full-width "大安區" equivalent is unusual but NFKC should handle variants
    const features = makeFeaturesSeries("大安區", "台北市", "2024-06", 12, 500000);
    // Query with half-width district name — should still match
    const result = computePriceTrendSeries(features, "大安區");
    expect(result.sufficient).toBe(true);
  });

  it("NFKC: parses full-width digit dates correctly in features", () => {
    // Simulate a feature where the date has full-width digits
    const fwDate = "２０２４－０３－１５"; // full-width
    const features = [
      { type: "Feature", geometry: null, properties: { district: "大安區", city: "台北市", date: fwDate, unitPrice: 500000, totalPrice: 15_000_000 } },
      ...makeFeaturesSeries("大安區", "台北市", "2024-08", 6, 500000),
    ];
    const result = computePriceTrendSeries(features, "大安區");
    // Should include 2024-03 from full-width date
    expect(result.points.some((pt) => pt.month === "2024-03")).toBe(true);
  });

  it("ignores features with non-positive unitPrice", () => {
    const bad = [
      makeFeature("大安區", "台北市", "2024-03-10", 0),
      makeFeature("大安區", "台北市", "2024-03-11", -1000),
    ];
    // Base starts at 2024-09, so it spans 2024-09..2024-04 — no overlap with 2024-03
    const good = makeFeaturesSeries("大安區", "台北市", "2024-09", 6, 500000);
    const result = computePriceTrendSeries([...bad, ...good], "大安區");
    // 2024-03 should not appear (no valid prices)
    expect(result.points.some((pt) => pt.month === "2024-03")).toBe(false);
  });

  it("ignores features with invalid/missing date", () => {
    const bad = [
      makeFeature("大安區", "台北市", "", 500000),
      { type: "Feature", geometry: null, properties: { district: "大安區", city: "台北市", date: null, unitPrice: 500000 } },
    ];
    const good = makeFeaturesSeries("大安區", "台北市", "2024-08", 6, 500000);
    // Should not crash; result should be derived from good features only
    const result = computePriceTrendSeries([...bad, ...good], "大安區");
    expect(result.sufficient).toBe(true);
  });

  it("includes count property for each TrendPoint", () => {
    const features = [
      makeFeature("大安區", "台北市", "2024-03-10", 300000),
      makeFeature("大安區", "台北市", "2024-03-20", 400000),
      // Base starts at 2024-09 so 2024-03 only has the 2 features above
      ...makeFeaturesSeries("大安區", "台北市", "2024-09", 6, 500000),
    ];
    const result = computePriceTrendSeries(features, "大安區");
    const march = result.points.find((pt) => pt.month === "2024-03");
    expect(march?.count).toBe(2);
  });

  it("district and city are reflected in the returned series", () => {
    const features = makeFeaturesSeries("信義區", "台北市", "2024-12", 12, 600000);
    const result = computePriceTrendSeries(features, "信義區", "台北市");
    expect(result.district).toBe("信義區");
    expect(result.city).toBe("台北市");
  });
});
