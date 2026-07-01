import { describe, it, expect } from "vitest";
import {
  computeDistrictRows,
  sortRows,
  computeBuyerTimingScore,
  compute6MonthSparkline,
  computeYoYPct,
  computeMedianPrice12mo,
  countTx12mo,
  type DistrictRow,
} from "../lib/districtTableUtils.js";

// ── Test data factory ─────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  city: string,
  unitPrice: number,
  date: string
): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, city, unitPrice, date, totalPrice: unitPrice * 50, areaPing: 50 },
  };
}

/** Generate N features for (district, city) evenly spread over the past 13 months. */
function makeDistrictFeatures(
  district: string,
  city: string,
  count: number,
  basePrice = 800_000,
  monthsBack = 13
): any[] {
  const feats: any[] = [];
  const now = new Date("2024-06-15");
  for (let i = 0; i < count; i++) {
    const daysBack = Math.floor((i / count) * monthsBack * 30);
    const d = new Date(now);
    d.setDate(d.getDate() - daysBack);
    const dateStr = d.toISOString().slice(0, 10);
    feats.push(makeFeature(district, city, basePrice + i * 1000, dateStr));
  }
  return feats;
}

// ── computeDistrictRows ───────────────────────────────────────────────────────

describe("computeDistrictRows", () => {
  it("returns empty array when features is empty", () => {
    expect(computeDistrictRows([])).toEqual([]);
  });

  it("excludes districts with fewer than minTx transactions in last 12 months", () => {
    const features = makeDistrictFeatures("中正區", "台北市", 5); // only 5 in 12mo window
    const rows = computeDistrictRows(features, 10);
    expect(rows).toHaveLength(0);
  });

  it("includes districts with exactly minTx transactions", () => {
    const features = makeDistrictFeatures("大安區", "台北市", 10, 900_000, 10);
    const rows = computeDistrictRows(features, 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.district).toBe("大安區");
    expect(rows[0]!.city).toBe("台北市");
  });

  it("returns correct district and city fields", () => {
    const features = [
      ...makeDistrictFeatures("信義區", "台北市", 15, 1_200_000, 10),
      ...makeDistrictFeatures("板橋區", "新北市", 12, 600_000, 10),
    ];
    const rows = computeDistrictRows(features, 10);
    expect(rows).toHaveLength(2);
    const districtNames = rows.map((r) => r.district).sort();
    expect(districtNames).toEqual(["信義區", "板橋區"].sort());
  });

  it("sets txCount12mo correctly", () => {
    const features = makeDistrictFeatures("中山區", "台北市", 20, 900_000, 10);
    const rows = computeDistrictRows(features, 10);
    expect(rows[0]!.txCount12mo).toBeGreaterThanOrEqual(10);
    expect(rows[0]!.txCount12mo).toBeLessThanOrEqual(20);
  });

  it("uses default minTx of 10", () => {
    const features = makeDistrictFeatures("南港區", "台北市", 9);
    expect(computeDistrictRows(features)).toHaveLength(0);
  });

  it("handles missing unitPrice gracefully (filters out 0/undefined)", () => {
    const validFeats = makeDistrictFeatures("文山區", "台北市", 12, 700_000, 10);
    // Add garbage feature with no unitPrice
    const badFeat = makeFeature("文山區", "台北市", 0, "2024-05-01");
    const rows = computeDistrictRows([...validFeats, badFeat], 10);
    expect(rows).toHaveLength(1);
  });
});

// ── sortRows ──────────────────────────────────────────────────────────────────

const SAMPLE_ROWS: DistrictRow[] = [
  { district: "大安區", city: "台北市", medianPriceWan: 150, yoyPct: 3.0, txCount12mo: 80, buyerTimingScore: 72, sparkline6mo: [140, 142, 145, 148, 150, 152] },
  { district: "信義區", city: "台北市", medianPriceWan: 200, yoyPct: -1.5, txCount12mo: 60, buyerTimingScore: 55, sparkline6mo: [195, 198, 200, 200, 200, 200] },
  { district: "板橋區", city: "新北市", medianPriceWan: 80, yoyPct: 5.0, txCount12mo: 120, buyerTimingScore: null, sparkline6mo: [75, 76, 78, 79, 80, 80] },
  { district: "中正區", city: "台北市", medianPriceWan: null, yoyPct: null, txCount12mo: 25, buyerTimingScore: 45, sparkline6mo: Array(6).fill(null) },
];

describe("sortRows", () => {
  it("sorts by timingScore desc (default — best deals first)", () => {
    const sorted = sortRows(SAMPLE_ROWS, "timingScore", "desc");
    // 72 > 55 > 45 > null
    expect(sorted[0]!.buyerTimingScore).toBe(72);
    expect(sorted[1]!.buyerTimingScore).toBe(55);
    expect(sorted[2]!.buyerTimingScore).toBe(45);
    expect(sorted[3]!.buyerTimingScore).toBeNull();
  });

  it("sorts by timingScore asc", () => {
    const sorted = sortRows(SAMPLE_ROWS, "timingScore", "asc");
    expect(sorted[0]!.buyerTimingScore).toBe(45);
    expect(sorted[1]!.buyerTimingScore).toBe(55);
    expect(sorted[2]!.buyerTimingScore).toBe(72);
    expect(sorted[3]!.buyerTimingScore).toBeNull(); // null still last
  });

  it("sorts by medianPrice asc", () => {
    const sorted = sortRows(SAMPLE_ROWS, "medianPrice", "asc");
    // 80 < 150 < 200; null last
    expect(sorted[0]!.medianPriceWan).toBe(80);
    expect(sorted[1]!.medianPriceWan).toBe(150);
    expect(sorted[2]!.medianPriceWan).toBe(200);
    expect(sorted[3]!.medianPriceWan).toBeNull();
  });

  it("sorts by medianPrice desc", () => {
    const sorted = sortRows(SAMPLE_ROWS, "medianPrice", "desc");
    expect(sorted[0]!.medianPriceWan).toBe(200);
    expect(sorted[1]!.medianPriceWan).toBe(150);
    expect(sorted[2]!.medianPriceWan).toBe(80);
    expect(sorted[3]!.medianPriceWan).toBeNull();
  });

  it("sorts by txCount desc (highest volume first)", () => {
    const sorted = sortRows(SAMPLE_ROWS, "txCount", "desc");
    expect(sorted[0]!.txCount12mo).toBe(120);
    expect(sorted[1]!.txCount12mo).toBe(80);
    expect(sorted[2]!.txCount12mo).toBe(60);
    expect(sorted[3]!.txCount12mo).toBe(25);
  });

  it("sorts by yoyPct asc (biggest decline first)", () => {
    const sorted = sortRows(SAMPLE_ROWS, "yoyPct", "asc");
    // -1.5 < 3.0 < 5.0; null last
    expect(sorted[0]!.yoyPct).toBe(-1.5);
    expect(sorted[1]!.yoyPct).toBe(3.0);
    expect(sorted[2]!.yoyPct).toBe(5.0);
    expect(sorted[3]!.yoyPct).toBeNull();
  });

  it("sorts by district name (zh-TW locale asc)", () => {
    const sorted = sortRows(SAMPLE_ROWS, "district", "asc");
    // All should be in some locale-consistent order — just check it doesn't crash and returns same count
    expect(sorted).toHaveLength(SAMPLE_ROWS.length);
    const cityDistrict = sorted.map((r) => `${r.city}${r.district}`);
    const expected = [...cityDistrict].sort((a, b) => a.localeCompare(b, "zh-TW"));
    expect(cityDistrict).toEqual(expected);
  });

  it("does not mutate the original array", () => {
    const original = [...SAMPLE_ROWS];
    sortRows(SAMPLE_ROWS, "timingScore", "desc");
    expect(SAMPLE_ROWS).toEqual(original);
  });
});

// ── computeBuyerTimingScore ───────────────────────────────────────────────────

describe("computeBuyerTimingScore", () => {
  it("returns null when fewer than 10 transactions in last 12 months", () => {
    const features = makeDistrictFeatures("松山區", "台北市", 5);
    expect(computeBuyerTimingScore(features, "松山區", "台北市")).toBeNull();
  });

  it("returns a number 0–100 with sufficient data", () => {
    const features = makeDistrictFeatures("內湖區", "台北市", 30, 900_000, 10);
    const score = computeBuyerTimingScore(features, "內湖區", "台北市");
    expect(score).not.toBeNull();
    expect(score!).toBeGreaterThanOrEqual(0);
    expect(score!).toBeLessThanOrEqual(100);
  });

  it("returns null for an unknown district", () => {
    const features = makeDistrictFeatures("大安區", "台北市", 20);
    expect(computeBuyerTimingScore(features, "不存在區", "台北市")).toBeNull();
  });

  it("gives higher score when prices are declining (buyer advantage)", () => {
    // Build features where recent prices are lower than prior year
    const now = new Date("2024-06-15");
    const recentFeats = Array.from({ length: 15 }, (_, i) => {
      const d = new Date(now);
      d.setMonth(d.getMonth() - i % 11);
      return makeFeature("士林區", "台北市", 600_000 - i * 5000, d.toISOString().slice(0, 10));
    });
    const priorFeats = Array.from({ length: 12 }, (_, i) => {
      const d = new Date(now);
      d.setFullYear(d.getFullYear() - 1);
      d.setMonth(d.getMonth() - i % 11);
      return makeFeature("士林區", "台北市", 800_000 + i * 5000, d.toISOString().slice(0, 10));
    });
    const rising = computeBuyerTimingScore([...recentFeats, ...priorFeats], "士林區", "台北市");
    expect(rising).not.toBeNull();
    expect(rising!).toBeGreaterThan(50); // declining prices → higher buyer score
  });
});

// ── compute6MonthSparkline ────────────────────────────────────────────────────

describe("compute6MonthSparkline", () => {
  it("returns array of length 6", () => {
    const features = makeDistrictFeatures("北投區", "台北市", 20, 800_000, 8);
    const spark = compute6MonthSparkline(features, "北投區", "台北市");
    expect(spark).toHaveLength(6);
  });

  it("returns 6 nulls for empty features", () => {
    expect(compute6MonthSparkline([], "北投區", "台北市")).toEqual(Array(6).fill(null));
  });

  it("values in 萬/坪 range (not raw 元)", () => {
    const features = makeDistrictFeatures("萬華區", "台北市", 20, 900_000, 6);
    const spark = compute6MonthSparkline(features, "萬華區", "台北市");
    const nonNull = spark.filter((v) => v !== null) as number[];
    expect(nonNull.length).toBeGreaterThan(0);
    // 900,000 元/坪 = 90 萬/坪 — should be in that range, not millions
    for (const v of nonNull) {
      expect(v).toBeGreaterThan(1);
      expect(v).toBeLessThan(1000);
    }
  });
});

// ── edge cases ────────────────────────────────────────────────────────────────

describe("edge cases", () => {
  it("computeDistrictRows handles null / undefined features gracefully", () => {
    expect(computeDistrictRows(null as any)).toEqual([]);
    expect(computeDistrictRows(undefined as any)).toEqual([]);
  });

  it("computeYoYPct returns null with no data", () => {
    expect(computeYoYPct([], "大同區", "台北市")).toBeNull();
  });

  it("computeMedianPrice12mo returns null with no data", () => {
    expect(computeMedianPrice12mo([], "大同區", "台北市")).toBeNull();
  });

  it("countTx12mo returns 0 for empty features", () => {
    expect(countTx12mo([], "大同區", "台北市")).toBe(0);
  });

  it("features from a different city are excluded when city is specified", () => {
    const tp  = makeDistrictFeatures("東區", "台北市", 15, 900_000, 10);
    const tn  = makeDistrictFeatures("東區", "台南市", 15, 400_000, 10);
    const rowsTaipei = computeDistrictRows(tp, 10);
    const rowsTainan = computeDistrictRows(tn, 10);
    // Same district name, different cities — rows should be distinct
    expect(rowsTaipei[0]!.city).toBe("台北市");
    expect(rowsTainan[0]!.city).toBe("台南市");
    // median prices should differ
    expect(rowsTaipei[0]!.medianPriceWan).not.toBe(rowsTainan[0]!.medianPriceWan);
  });
});
