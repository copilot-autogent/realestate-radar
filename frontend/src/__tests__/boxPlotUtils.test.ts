import { describe, it, expect } from "vitest";
import {
  computeBoxPlot,
  computePercentileRank,
  computeDistrictBoxPlots,
  parseDateYear,
  type BoxPlotStats,
} from "../lib/boxPlotUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  city: string,
  date: string,
  unitPrice: number
): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, city, date, unitPrice },
  };
}

function prices(...vals: number[]): Array<{ unitPrice: number }> {
  return vals.map((v) => ({ unitPrice: v }));
}

function bp(median: number): BoxPlotStats {
  return {
    yearMonth: "x",
    min: median,
    q1: median,
    median,
    q3: median,
    max: median,
    count: 1,
  };
}

// ── parseDateYear ─────────────────────────────────────────────────────────────

describe("parseDateYear", () => {
  it("parses YYYY-MM-DD format", () => {
    expect(parseDateYear("2024-03-15")).toBe(2024);
  });

  it("parses YYYY/MM/DD format", () => {
    expect(parseDateYear("2023/07/01")).toBe(2023);
  });

  it("normalises full-width CJK digits (Taiwan gov data)", () => {
    // ２０２４ are U+FF12 U+FF10 U+FF12 U+FF14 (full-width digits)
    expect(parseDateYear("\uFF12\uFF10\uFF12\uFF14-\uFF10\uFF11-\uFF11\uFF15")).toBe(2024);
  });

  it("returns null for null input", () => {
    expect(parseDateYear(null)).toBeNull();
  });

  it("returns null for non-string input", () => {
    expect(parseDateYear(20240315)).toBeNull();
  });

  it("returns null for malformed date string", () => {
    expect(parseDateYear("not-a-date")).toBeNull();
  });

  it("returns null for empty string", () => {
    expect(parseDateYear("")).toBeNull();
  });
});

// ── computeBoxPlot ────────────────────────────────────────────────────────────

describe("computeBoxPlot", () => {
  it("returns null for empty array", () => {
    expect(computeBoxPlot([], "2024")).toBeNull();
  });

  it("returns null when all unitPrices are invalid (negative, zero, NaN)", () => {
    expect(
      computeBoxPlot([{ unitPrice: -1 }, { unitPrice: 0 }, { unitPrice: NaN }], "2024")
    ).toBeNull();
  });

  it("single transaction: all 5-number values are equal", () => {
    const result = computeBoxPlot(prices(200000), "2024")!;
    expect(result).not.toBeNull();
    expect(result.min).toBe(200000);
    expect(result.q1).toBe(200000);
    expect(result.median).toBe(200000);
    expect(result.q3).toBe(200000);
    expect(result.max).toBe(200000);
    expect(result.count).toBe(1);
  });

  it("yearMonth label is preserved", () => {
    const result = computeBoxPlot(prices(100000), "2025-Q1")!;
    expect(result.yearMonth).toBe("2025-Q1");
  });

  it("two transactions: median is average of both", () => {
    const result = computeBoxPlot(prices(200000, 400000), "2023")!;
    expect(result.median).toBe(300000);
    expect(result.min).toBe(200000);
    expect(result.max).toBe(400000);
  });

  it("odd count: median is exact middle value", () => {
    // sorted: [100, 200, 300, 400, 500]
    const result = computeBoxPlot(prices(500, 100, 300, 200, 400), "2022")!;
    expect(result.median).toBe(300);
  });

  it("even count: median is average of two middle values", () => {
    // sorted: [100, 200, 300, 400] → median = 250
    const result = computeBoxPlot(prices(100, 200, 300, 400), "2022")!;
    expect(result.median).toBe(250);
  });

  it("all equal values (ties at quartile boundaries): all stats equal", () => {
    const result = computeBoxPlot(prices(100, 100, 100, 100), "2022")!;
    expect(result.min).toBe(100);
    expect(result.q1).toBe(100);
    expect(result.median).toBe(100);
    expect(result.q3).toBe(100);
    expect(result.max).toBe(100);
  });

  it("spread data: q1 < median < q3", () => {
    const result = computeBoxPlot(prices(100, 200, 300, 400, 500, 600, 700), "2024")!;
    expect(result.q1).toBeLessThan(result.median);
    expect(result.median).toBeLessThan(result.q3);
  });

  it("ignores invalid entries and counts only valid prices", () => {
    const result = computeBoxPlot(
      [{ unitPrice: 100000 }, { unitPrice: -1 }, { unitPrice: 200000 }, { unitPrice: 0 }],
      "2024"
    )!;
    expect(result.count).toBe(2);
    expect(result.median).toBe(150000);
  });

  it("min ≤ q1 ≤ median ≤ q3 ≤ max for any valid input", () => {
    const result = computeBoxPlot(prices(50, 120, 250, 300, 310, 400, 500, 600), "2024")!;
    expect(result.min).toBeLessThanOrEqual(result.q1);
    expect(result.q1).toBeLessThanOrEqual(result.median);
    expect(result.median).toBeLessThanOrEqual(result.q3);
    expect(result.q3).toBeLessThanOrEqual(result.max);
  });
});

// ── computePercentileRank ─────────────────────────────────────────────────────

describe("computePercentileRank", () => {
  it("returns 50 for empty historical list (neutral fallback)", () => {
    expect(computePercentileRank(250000, [])).toBe(50);
  });

  it("returns 0 when current is below all historical medians", () => {
    expect(computePercentileRank(100, [bp(200), bp(300), bp(400)])).toBe(0);
  });

  it("returns 100 when current is above all historical medians", () => {
    expect(computePercentileRank(500, [bp(200), bp(300), bp(400)])).toBe(100);
  });

  it("returns 50 for exact midpoint in symmetric 3-value distribution", () => {
    // medians: [100, 200, 300] — current=200 → 50th percentile
    expect(computePercentileRank(200, [bp(100), bp(200), bp(300)])).toBe(50);
  });

  it("interpolates fractional rank: midpoint of two values", () => {
    // medians: [100, 300] — current=200 → 50th
    expect(computePercentileRank(200, [bp(100), bp(300)])).toBe(50);
  });

  it("returns 100 for single historical value when current >= it", () => {
    expect(computePercentileRank(300, [bp(300)])).toBe(100);
  });

  it("returns 0 for single historical value when current < it", () => {
    expect(computePercentileRank(100, [bp(300)])).toBe(0);
  });

  it("result is always within [0, 100] for any input", () => {
    const histPlots = [bp(100), bp(200), bp(300), bp(400)];
    for (let v = 0; v <= 500; v += 50) {
      const rank = computePercentileRank(v, histPlots);
      expect(rank).toBeGreaterThanOrEqual(0);
      expect(rank).toBeLessThanOrEqual(100);
    }
  });

  it("higher current median yields higher or equal rank", () => {
    const histPlots = [bp(150), bp(250), bp(350)];
    const rank200 = computePercentileRank(200, histPlots);
    const rank300 = computePercentileRank(300, histPlots);
    expect(rank300).toBeGreaterThanOrEqual(rank200);
  });
});

// ── computeDistrictBoxPlots ───────────────────────────────────────────────────

describe("computeDistrictBoxPlots", () => {
  it("returns sufficient=false and empty plots for empty features array", () => {
    const result = computeDistrictBoxPlots([], "大安區");
    expect(result.sufficient).toBe(false);
    expect(result.plots).toHaveLength(0);
    expect(result.percentileRank).toBeNull();
  });

  it("returns sufficient=false when only one year of data exists", () => {
    const features = Array.from({ length: 5 }, () =>
      makeFeature("大安區", "台北市", "2024-06-15", 300000)
    );
    const result = computeDistrictBoxPlots(features, "大安區", "台北市");
    expect(result.sufficient).toBe(false);
  });

  it("filters by district — different districts excluded", () => {
    const good = Array.from({ length: 5 }, () =>
      makeFeature("大安區", "台北市", "2023-06-15", 300000)
    );
    const bad = Array.from({ length: 5 }, () =>
      makeFeature("信義區", "台北市", "2023-06-15", 500000)
    );
    const result = computeDistrictBoxPlots([...good, ...bad], "大安區", "台北市");
    // Insufficient (only 1 year) but verify no 信義區 data leaked in
    expect(result.plots.length).toBeGreaterThan(0);
    expect(result.plots.every((p) => p.median === 300000)).toBe(true);
  });

  it("sufficient=true with 2+ years of data, percentileRank is non-null", () => {
    const features = [
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2023-06-15", 280000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2024-06-15", 300000)
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
    expect(result.percentileRank).not.toBeNull();
  });

  it("3-year rollup produces exactly 3 plots", () => {
    const features = [
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2022-06-15", 250000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2023-06-15", 280000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2024-06-15", 300000)
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市", 3);
    expect(result.plots).toHaveLength(3);
  });

  it("currentMedian matches the most recent year's median", () => {
    const features = [
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2023-06-15", 280000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2024-06-15", 300000)
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市");
    expect(result.currentMedian).toBe(300000);
  });

  it("handles full-width CJK digit dates (NFKC normalization)", () => {
    // ２０２３-０６-１５ and ２０２４-０６-１５ using full-width digits
    const features = [
      makeFeature(
        "大安區",
        "台北市",
        "\uFF12\uFF10\uFF12\uFF13-\uFF10\uFF16-\uFF11\uFF15",
        280000
      ),
      makeFeature(
        "大安區",
        "台北市",
        "\uFF12\uFF10\uFF12\uFF14-\uFF10\uFF16-\uFF11\uFF15",
        300000
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市");
    expect(result.sufficient).toBe(true);
  });

  it("plots are sorted by year in ascending order", () => {
    const features = [
      ...Array.from({ length: 3 }, () =>
        makeFeature("大安區", "台北市", "2024-01-01", 300000)
      ),
      ...Array.from({ length: 3 }, () =>
        makeFeature("大安區", "台北市", "2022-01-01", 250000)
      ),
      ...Array.from({ length: 3 }, () =>
        makeFeature("大安區", "台北市", "2023-01-01", 275000)
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市", 3);
    const years = result.plots.map((p) => Number(p.yearMonth));
    expect(years).toEqual([...years].sort((a, b) => a - b));
  });

  it("ignores transactions from outside the rolling window", () => {
    // Only 2022–2024 window (maxYears=3); 2019 should be excluded
    const features = [
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2019-06-15", 150000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2023-06-15", 280000)
      ),
      ...Array.from({ length: 5 }, () =>
        makeFeature("大安區", "台北市", "2024-06-15", 300000)
      ),
    ];
    const result = computeDistrictBoxPlots(features, "大安區", "台北市", 3);
    expect(result.plots.every((p) => Number(p.yearMonth) >= 2022)).toBe(true);
  });
});
