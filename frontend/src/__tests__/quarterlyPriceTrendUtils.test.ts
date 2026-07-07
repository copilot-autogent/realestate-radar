import { describe, it, expect } from "vitest";
import {
  parseDateQuarter,
  quarterLabel,
  computeQuarterlyYoY,
  yoyToTrendColor,
  formatYoYBadge,
  computeQuarterlyPriceSeries,
  type QuarterlyPriceSeries,
} from "../lib/quarterlyPriceTrendUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Build a minimal GeoJSON feature for a transaction. unitPrice is NT$/坪. */
function makeFeature(
  district: string,
  city: string,
  date: string,
  unitPriceNtd: number,
): { properties: Record<string, unknown> } {
  return {
    properties: { district, city, date, unitPrice: unitPriceNtd },
  };
}

// ── parseDateQuarter ──────────────────────────────────────────────────────────

describe("parseDateQuarter", () => {
  it("parses YYYY-MM-DD format", () => {
    expect(parseDateQuarter("2024-03-15")).toEqual({ year: 2024, quarter: 1 });
    expect(parseDateQuarter("2024-04-01")).toEqual({ year: 2024, quarter: 2 });
    expect(parseDateQuarter("2024-07-20")).toEqual({ year: 2024, quarter: 3 });
    expect(parseDateQuarter("2024-10-10")).toEqual({ year: 2024, quarter: 4 });
  });

  it("parses YYYY/MM/DD format", () => {
    expect(parseDateQuarter("2025/01/05")).toEqual({ year: 2025, quarter: 1 });
    expect(parseDateQuarter("2025/06/30")).toEqual({ year: 2025, quarter: 2 });
  });

  it("parses YYYYMMDD format", () => {
    expect(parseDateQuarter("20241215")).toEqual({ year: 2024, quarter: 4 });
    expect(parseDateQuarter("20250301")).toEqual({ year: 2025, quarter: 1 });
  });

  it("normalises full-width CJK digits before parsing", () => {
    // Full-width: ２０２４－０３－１５
    expect(parseDateQuarter("２０２４－０３－１５")).toEqual({ year: 2024, quarter: 1 });
    expect(parseDateQuarter("２０２４／０７／２０")).toEqual({ year: 2024, quarter: 3 });
  });

  it("returns null for invalid inputs", () => {
    expect(parseDateQuarter(null)).toBeNull();
    expect(parseDateQuarter(undefined)).toBeNull();
    expect(parseDateQuarter(12345)).toBeNull();
    expect(parseDateQuarter("")).toBeNull();
    expect(parseDateQuarter("not-a-date")).toBeNull();
    // year > 2100 check
    expect(parseDateQuarter("9999-01-01")).toBeNull();
  });

  it("returns null for months out of range", () => {
    expect(parseDateQuarter("2024-00-01")).toBeNull();
    expect(parseDateQuarter("2024-13-01")).toBeNull();
  });
});

// ── quarterLabel ──────────────────────────────────────────────────────────────

describe("quarterLabel", () => {
  it("formats correctly", () => {
    expect(quarterLabel(2024, 1)).toBe("2024-Q1");
    expect(quarterLabel(2024, 4)).toBe("2024-Q4");
    expect(quarterLabel(2025, 2)).toBe("2025-Q2");
  });
});

// ── computeQuarterlyYoY ───────────────────────────────────────────────────────

describe("computeQuarterlyYoY", () => {
  it("computes percentage change", () => {
    expect(computeQuarterlyYoY(55, 50)).toBeCloseTo(10, 5);
    expect(computeQuarterlyYoY(47.5, 50)).toBeCloseTo(-5, 5);
  });

  it("returns null for zero prior", () => {
    expect(computeQuarterlyYoY(50, 0)).toBeNull();
  });

  it("returns null for non-finite values", () => {
    expect(computeQuarterlyYoY(NaN, 50)).toBeNull();
    expect(computeQuarterlyYoY(50, Infinity)).toBeNull();
  });
});

// ── yoyToTrendColor ───────────────────────────────────────────────────────────

describe("yoyToTrendColor", () => {
  it("returns green when YoY ≤ -2%", () => {
    expect(yoyToTrendColor(-2)).toBe("green");
    expect(yoyToTrendColor(-5.1)).toBe("green");
    expect(yoyToTrendColor(-2.0)).toBe("green");
  });

  it("returns red when YoY ≥ +2%", () => {
    expect(yoyToTrendColor(2)).toBe("red");
    expect(yoyToTrendColor(3.2)).toBe("red");
  });

  it("returns grey within ±2% or null", () => {
    expect(yoyToTrendColor(1.9)).toBe("grey");
    expect(yoyToTrendColor(-1.9)).toBe("grey");
    expect(yoyToTrendColor(0)).toBe("grey");
    expect(yoyToTrendColor(null)).toBe("grey");
  });
});

// ── formatYoYBadge ────────────────────────────────────────────────────────────

describe("formatYoYBadge", () => {
  it("formats positive change with + sign and ↑", () => {
    expect(formatYoYBadge(3.2)).toBe("+3.2% YoY ↑");
  });

  it("formats negative change with ↓", () => {
    expect(formatYoYBadge(-5.1)).toBe("-5.1% YoY ↓");
  });

  it("formats zero and near-zero as flat (→)", () => {
    expect(formatYoYBadge(0)).toBe("0.0% YoY →");
    // Values rounding to 0.0 also use the flat branch
    expect(formatYoYBadge(0.04)).toBe("0.0% YoY →");
    expect(formatYoYBadge(-0.04)).toBe("0.0% YoY →");
  });

  it("returns empty string for null", () => {
    expect(formatYoYBadge(null)).toBe("");
  });
});

// ── computeQuarterlyPriceSeries ───────────────────────────────────────────────

/** Generate N transactions for a district in a given quarter with uniform price (NT$/坪). */
function txRange(
  district: string,
  city: string,
  year: number,
  quarter: 1 | 2 | 3 | 4,
  count: number,
  unitPriceNtd: number,
): ReturnType<typeof makeFeature>[] {
  const monthInQ = quarter * 3 - 1; // middle month of the quarter
  const date = `${year}-${String(monthInQ).padStart(2, "0")}-15`;
  return Array.from({ length: count }, () => makeFeature(district, city, date, unitPriceNtd));
}

describe("computeQuarterlyPriceSeries", () => {
  it("returns empty/insufficient series for empty features", () => {
    const result = computeQuarterlyPriceSeries([], "大安區", "台北市");
    expect(result.sufficient).toBe(false);
    expect(result.points).toHaveLength(0);
  });

  it("falls back to all-Taiwan when district has too few transactions", () => {
    // Only 5 transactions for 大安區 — below MIN_QUARTER_TX (10)
    const features = txRange("大安區", "台北市", 2024, 2, 5, 500000);
    // Add 20 transactions spread across 5 quarters for other districts
    const fallback = [
      ...txRange("其他區", "台北市", 2024, 1, 20, 400000),
      ...txRange("其他區", "台北市", 2024, 2, 20, 410000),
      ...txRange("其他區", "台北市", 2024, 3, 20, 420000),
      ...txRange("其他區", "台北市", 2024, 4, 20, 415000),
      ...txRange("其他區", "台北市", 2025, 1, 20, 430000),
    ];
    const result = computeQuarterlyPriceSeries([...features, ...fallback], "大安區", "台北市");
    expect(result.isFallback).toBe(true);
    expect(result.district).toBe("大安區");
  });

  it("falls back when district has 10+ transactions but concentrated in < MIN_QUARTERS quarters", () => {
    // 30 transactions for 大安區 all in one quarter — produces only 1 qualifying quarter
    const features = txRange("大安區", "台北市", 2024, 2, 30, 500000);
    const fallback = [
      ...txRange("其他區", "台北市", 2024, 1, 20, 400000),
      ...txRange("其他區", "台北市", 2024, 2, 20, 410000),
      ...txRange("其他區", "台北市", 2024, 3, 20, 420000),
      ...txRange("其他區", "台北市", 2024, 4, 20, 415000),
    ];
    const result = computeQuarterlyPriceSeries([...features, ...fallback], "大安區", "台北市");
    expect(result.isFallback).toBe(true);
  });

  it("returns district series when district has sufficient data", () => {
    // 4 quarters with 10+ transactions each
    const features = [
      ...txRange("大安區", "台北市", 2024, 1, 12, 600000),
      ...txRange("大安區", "台北市", 2024, 2, 12, 620000),
      ...txRange("大安區", "台北市", 2024, 3, 12, 610000),
      ...txRange("大安區", "台北市", 2024, 4, 12, 580000),
    ];
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    expect(result.isFallback).toBe(false);
    expect(result.sufficient).toBe(true);
    expect(result.points.length).toBeGreaterThanOrEqual(4);
  });

  it("converts NT$/坪 to 萬/坪 correctly", () => {
    const features = [
      ...txRange("大安區", "台北市", 2024, 1, 12, 600000), // 60 萬/坪
      ...txRange("大安區", "台北市", 2024, 2, 12, 500000), // 50 萬/坪
      ...txRange("大安區", "台北市", 2024, 3, 12, 550000), // 55 萬/坪
      ...txRange("大安區", "台北市", 2024, 4, 12, 520000), // 52 萬/坪
    ];
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    expect(result.points[0]!.medianPerPing).toBeCloseTo(60, 2);
    expect(result.points[1]!.medianPerPing).toBeCloseTo(50, 2);
  });

  it("computes YoY badge between current quarter and same quarter last year", () => {
    // Q2-2024: 500000 NT$/坪 (50 萬/坪), Q2-2025: 525000 NT$/坪 (52.5 萬/坪) → +5% YoY
    const features = [
      ...txRange("大安區", "台北市", 2024, 1, 12, 500000),
      ...txRange("大安區", "台北市", 2024, 2, 12, 500000),
      ...txRange("大安區", "台北市", 2024, 3, 12, 500000),
      ...txRange("大安區", "台北市", 2024, 4, 12, 500000),
      ...txRange("大安區", "台北市", 2025, 1, 12, 500000),
      ...txRange("大安區", "台北市", 2025, 2, 12, 525000), // +5% YoY vs Q2-2024
    ];
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    const lastQ = result.points[result.points.length - 1]!;
    expect(lastQ.label).toBe("2025-Q2");
    expect(result.yoyPct).toBeCloseTo(5, 1);
    expect(result.yoyBadge).toBe("+5.0% YoY ↑");
    expect(result.trendColor).toBe("red"); // rising ≥ +2%
  });

  it("marks trendColor green for falling prices (YoY ≤ -2%)", () => {
    const features = [
      ...txRange("大安區", "台北市", 2024, 1, 12, 600000),
      ...txRange("大安區", "台北市", 2024, 2, 12, 600000),
      ...txRange("大安區", "台北市", 2024, 3, 12, 600000),
      ...txRange("大安區", "台北市", 2024, 4, 12, 600000),
      ...txRange("大安區", "台北市", 2025, 1, 12, 600000),
      ...txRange("大安區", "台北市", 2025, 2, 12, 570000), // -5% YoY
    ];
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    expect(result.trendColor).toBe("green");
    expect(result.yoyBadge).toBe("-5.0% YoY ↓");
  });

  it("returns null YoY when prior-year quarter is absent", () => {
    const features = [
      ...txRange("大安區", "台北市", 2025, 1, 12, 500000),
      ...txRange("大安區", "台北市", 2025, 2, 12, 510000),
      ...txRange("大安區", "台北市", 2025, 3, 12, 520000),
      ...txRange("大安區", "台北市", 2025, 4, 12, 530000),
    ];
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    expect(result.yoyPct).toBeNull();
    expect(result.yoyBadge).toBe("");
    expect(result.trendColor).toBe("grey");
  });

  it("keeps at most MAX_QUARTERS (8) points", () => {
    const features = [];
    for (let q = 1; q <= 4; q++) {
      for (let y = 2022; y <= 2025; y++) {
        features.push(...txRange("大安區", "台北市", y, q as 1 | 2 | 3 | 4, 12, 500000));
      }
    }
    const result = computeQuarterlyPriceSeries(features as any[], "大安區", "台北市");
    expect(result.points.length).toBeLessThanOrEqual(8);
  });

  it("handles NFKC-normalised (full-width) dates in features", () => {
    // Full-width date in the feature
    const f = makeFeature("大安區", "台北市", "２０２４－０２－１５", 500000);
    const features = Array.from({ length: 12 }, () => f);
    const result = computeQuarterlyPriceSeries(features, "大安區", "台北市");
    // The quarter should be parsed correctly (Feb → Q1)
    expect(result.points.some((p) => p.label === "2024-Q1")).toBe(true);
  });

  it("uses all-Taiwan when no district is passed", () => {
    const features = [
      ...txRange("大安區", "台北市", 2024, 1, 20, 600000),
      ...txRange("信義區", "台北市", 2024, 1, 20, 700000),
      ...txRange("大安區", "台北市", 2024, 2, 20, 620000),
      ...txRange("信義區", "台北市", 2024, 2, 20, 720000),
      ...txRange("大安區", "台北市", 2024, 3, 20, 610000),
      ...txRange("信義區", "台北市", 2024, 3, 20, 710000),
      ...txRange("大安區", "台北市", 2024, 4, 20, 600000),
      ...txRange("信義區", "台北市", 2024, 4, 20, 700000),
    ];
    const result = computeQuarterlyPriceSeries(features, "");
    expect(result.isFallback).toBe(true);
    expect(result.sufficient).toBe(true);
  });
});
