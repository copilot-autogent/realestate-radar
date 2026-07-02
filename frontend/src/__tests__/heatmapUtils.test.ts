import { describe, it, expect } from "vitest";
import { computeHeatmapData, last12Cells, type HeatmapCell } from "../lib/heatmapUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  city: string,
  date: string,
  unitPrice = 300000
): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, city, date, unitPrice },
  };
}

/**
 * Generate features spread across N months starting from startYYYYMM (e.g. "2024-01"),
 * with `countPerMonth` features per month, for the given district/city.
 */
function makeMonthlyFeatures(
  district: string,
  city: string,
  startYYYYMM: string,
  numMonths: number,
  countPerMonth: number,
  unitPrice = 300000
): any[] {
  const features: any[] = [];
  let [y, mo] = startYYYYMM.split("-").map(Number) as [number, number];
  for (let m = 0; m < numMonths; m++) {
    const dateStr = `${y}-${String(mo).padStart(2, "0")}-15`;
    for (let c = 0; c < countPerMonth; c++) {
      features.push(makeFeature(district, city, dateStr, unitPrice));
    }
    mo++;
    if (mo > 12) { mo = 1; y++; }
  }
  return features;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("computeHeatmapData", () => {
  it("returns sufficient=false when < 12 distinct (year, month) pairs", () => {
    const features = makeMonthlyFeatures("大安區", "台北市", "2024-01", 10, 3);
    const result = computeHeatmapData(features, "大安區", "台北市");
    expect(result.sufficient).toBe(false);
    expect(result.cells).toHaveLength(0);
  });

  it("returns sufficient=true when ≥ 12 distinct months", () => {
    const features = makeMonthlyFeatures("大安區", "台北市", "2023-01", 36, 2);
    const result = computeHeatmapData(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.years).toHaveLength(3);
  });

  it("filters by district and city correctly", () => {
    const goodFeatures = makeMonthlyFeatures("大安區", "台北市", "2023-01", 36, 2);
    const badFeatures  = makeMonthlyFeatures("信義區", "台北市", "2023-01", 36, 5);
    const result = computeHeatmapData([...goodFeatures, ...badFeatures], "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    const maxCount = Math.max(...result.cells.map((c) => c.count));
    expect(maxCount).toBe(2); // only 大安區 features included
  });

  it("produces 36 cells for a 3-year window", () => {
    const features = makeMonthlyFeatures("大安區", "台北市", "2022-01", 36, 3);
    const result = computeHeatmapData(features, "大安區", "台北市");
    expect(result.cells).toHaveLength(36);
  });

  it("computes medianUnitPriceWan correctly", () => {
    // 2 features with unit prices 200000 and 400000 → median 300000 → 30 萬
    const features = [
      makeFeature("大安區", "台北市", "2023-01-15", 200000),
      makeFeature("大安區", "台北市", "2023-01-16", 400000),
      // Fill out the remaining 35 months to meet ≥12 threshold
      ...makeMonthlyFeatures("大安區", "台北市", "2023-02", 35, 1, 300000),
    ];
    const result = computeHeatmapData(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    const jan2023 = result.cells.find((c) => c.year === 2023 && c.month === 1);
    expect(jan2023).toBeDefined();
    expect(jan2023!.medianUnitPriceWan).toBe(30); // 300000 / 10000 = 30
  });

  it("marks isBuyerAdvantaged when count < 70% of yearly peak", () => {
    // Make Jan–Nov 2024 have count=10 each, Dec 2024 have count=1 → Dec is advantaged
    const features: any[] = [];
    for (let mo = 1; mo <= 11; mo++) {
      const date = `2024-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 10; c++) features.push(makeFeature("大安區", "台北市", date));
    }
    // Only 1 transaction in Dec 2024 (< 7 = 70% of 10)
    features.push(makeFeature("大安區", "台北市", "2024-12-15"));
    // Add more months to reach ≥12 distinct pairs (add 2022 + 2023 months)
    features.push(...makeMonthlyFeatures("大安區", "台北市", "2022-01", 24, 1));

    const result = computeHeatmapData(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    const dec2024 = result.cells.find((c) => c.year === 2024 && c.month === 12);
    expect(dec2024).toBeDefined();
    expect(dec2024!.isBuyerAdvantaged).toBe(true);

    const jan2024 = result.cells.find((c) => c.year === 2024 && c.month === 1);
    expect(jan2024!.isBuyerAdvantaged).toBe(false);
  });

  it("global aggregate includes all districts when district is null", () => {
    const featA = makeMonthlyFeatures("大安區", "台北市", "2022-01", 36, 2);
    const featB = makeMonthlyFeatures("信義區", "台北市", "2022-01", 36, 3);
    const result = computeHeatmapData([...featA, ...featB], null);
    expect(result.isGlobal).toBe(true);
    expect(result.district).toBe("全臺灣");
    // Each month should have count=5 (2+3)
    const jan2022 = result.cells.find((c) => c.year === 2022 && c.month === 1);
    expect(jan2022!.count).toBe(5);
  });

  it("handles full-width digit date strings (NFKC normalisation)", () => {
    const features = [
      // Full-width digits: ２０２３-０１-１５
      makeFeature("中山區", "台北市", "２０２３-０１-１５"),
      ...makeMonthlyFeatures("中山區", "台北市", "2023-02", 35, 1),
    ];
    const result = computeHeatmapData(features, "中山區", "台北市");
    expect(result.sufficient).toBe(true);
    const jan = result.cells.find((c) => c.year === 2023 && c.month === 1);
    expect(jan).toBeDefined();
    expect(jan!.count).toBe(1);
  });
});

describe("last12Cells", () => {
  it("returns empty array for insufficient data", () => {
    const data = computeHeatmapData([], "大安區");
    expect(last12Cells(data)).toHaveLength(0);
  });

  it("returns exactly 12 cells", () => {
    const features = makeMonthlyFeatures("大安區", "台北市", "2022-01", 36, 2);
    const data = computeHeatmapData(features, "大安區", "台北市");
    expect(last12Cells(data)).toHaveLength(12);
  });

  it("last cell matches the most recent data month", () => {
    // 36 months from 2022-01 → last data is 2024-12
    const features = makeMonthlyFeatures("大安區", "台北市", "2022-01", 36, 2);
    const data = computeHeatmapData(features, "大安區", "台北市");
    const cells = last12Cells(data);
    const last = cells[cells.length - 1]!;
    expect(last.year).toBe(2024);
    expect(last.month).toBe(12);
  });

  it("fills count=0 for months with no transactions", () => {
    // 36 months with a gap: skip month 2023-06
    const features: any[] = [];
    for (let mo = 1; mo <= 12; mo++) {
      if (mo !== 6) {
        const date = `2023-${String(mo).padStart(2, "0")}-15`;
        for (let c = 0; c < 2; c++) features.push(makeFeature("大安區", "台北市", date));
      }
    }
    features.push(...makeMonthlyFeatures("大安區", "台北市", "2022-01", 12, 2));
    features.push(...makeMonthlyFeatures("大安區", "台北市", "2024-01", 12, 2));

    const data = computeHeatmapData(features, "大安區", "台北市");
    const cells = last12Cells(data);
    const jun2023 = cells.find((c) => c.year === 2024 && c.month === 6);
    // 2024-06 should have count=2 (gap is in 2023-06, not 2024-06)
    expect(jun2023?.count ?? 0).toBe(2);
  });
});
