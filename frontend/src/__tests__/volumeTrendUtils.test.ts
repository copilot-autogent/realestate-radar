import { describe, it, expect } from "vitest";
import {
  parseTransactionDate,
  computeVolumeYoY,
  computeYTDSummary,
  formatYoYPct,
  type QuarterlyVolume,
} from "../lib/volumeTrendUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(
  district: string,
  date: string,
  city = "台北市",
  props: Record<string, unknown> = {},
) {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district, date, city, unitPrice: 500000, ...props },
  };
}

/** Build N features all in one year-quarter */
function makeFeatures(district: string, year: number, month: number, count: number, city = "台北市") {
  return Array.from({ length: count }, (_, i) =>
    makeFeature(district, `${year}-${String(month).padStart(2, "0")}-${String(i + 1).padStart(2, "0")}`, city),
  );
}

// ── parseTransactionDate ──────────────────────────────────────────────────────

describe("parseTransactionDate", () => {
  it("parses standard YYYY-MM-DD", () => {
    expect(parseTransactionDate("2024-03-15")).toEqual({ year: 2024, month: 3 });
  });

  it("parses YYYY/MM/DD", () => {
    expect(parseTransactionDate("2025/07/04")).toEqual({ year: 2025, month: 7 });
  });

  it("parses single-digit month (YYYY-M-DD)", () => {
    expect(parseTransactionDate("2024-3-5")).toEqual({ year: 2024, month: 3 });
  });

  it("parses YYYYMMDD compact form", () => {
    expect(parseTransactionDate("20240615")).toEqual({ year: 2024, month: 6 });
  });

  it("parses Date object", () => {
    expect(parseTransactionDate(new Date("2024-04-01"))).toEqual({ year: 2024, month: 4 });
  });

  it("returns null for invalid Date object", () => {
    expect(parseTransactionDate(new Date("not-a-date"))).toBeNull();
  });

  it("returns null for null input", () => {
    expect(parseTransactionDate(null)).toBeNull();
  });

  it("returns null for undefined", () => {
    expect(parseTransactionDate(undefined)).toBeNull();
  });

  it("returns null for non-string garbage", () => {
    expect(parseTransactionDate(12345)).toBeNull();
  });

  it("returns null for out-of-range month", () => {
    expect(parseTransactionDate("2024-13-01")).toBeNull();
  });

  // NFKC normalisation — full-width digits (U+FF10–FF19)
  it("NFKC: parses full-width year digits ２０２４－０３－１５", () => {
    expect(parseTransactionDate("２０２４－０３－１５")).toEqual({ year: 2024, month: 3 });
  });

  it("NFKC: parses mixed full-width/half-width digits", () => {
    // '２024-０3-15' — year starts with full-width 2
    expect(parseTransactionDate("２024-０3-15")).toEqual({ year: 2024, month: 3 });
  });

  it("NFKC: full-width slash separator ２０２４／０６／０１", () => {
    expect(parseTransactionDate("２０２４／０６／０１")).toEqual({ year: 2024, month: 6 });
  });
});

// ── computeVolumeYoY — quarter bucketing ─────────────────────────────────────

describe("computeVolumeYoY — quarter bucketing", () => {
  const REF = 2024;

  it("always returns exactly 4 quarters", () => {
    const result = computeVolumeYoY([], "大安區", REF);
    expect(result).toHaveLength(4);
    expect(result.map((v) => v.quarter)).toEqual(["Q1", "Q2", "Q3", "Q4"]);
  });

  it("buckets January–March into Q1", () => {
    const features = [
      ...makeFeatures("大安區", REF, 1, 5),
      ...makeFeatures("大安區", REF, 2, 5),
      ...makeFeatures("大安區", REF, 3, 5),
    ];
    const [q1] = computeVolumeYoY(features, "大安區", REF);
    expect(q1!.currentYear).toBe(15);
  });

  it("buckets April–June into Q2", () => {
    const features = makeFeatures("大安區", REF, 5, 8);
    const result = computeVolumeYoY(features, "大安區", REF);
    expect(result[1]!.currentYear).toBe(8); // Q2
  });

  it("buckets July–September into Q3", () => {
    const features = makeFeatures("大安區", REF, 8, 6);
    const result = computeVolumeYoY(features, "大安區", REF);
    expect(result[2]!.currentYear).toBe(6); // Q3
  });

  it("buckets October–December into Q4", () => {
    const features = makeFeatures("大安區", REF, 11, 4);
    const result = computeVolumeYoY(features, "大安區", REF);
    expect(result[3]!.currentYear).toBe(4); // Q4
  });
});

// ── computeVolumeYoY — prior year (cross-year boundary) ──────────────────────

describe("computeVolumeYoY — prior year / cross-year boundary", () => {
  it("counts prior year transactions separately", () => {
    const features = [
      ...makeFeatures("信義區", 2024, 4, 10), // current year Q2
      ...makeFeatures("信義區", 2023, 4, 7),  // prior year Q2
    ];
    const result = computeVolumeYoY(features, "信義區", 2024);
    expect(result[1]!.currentYear).toBe(10);
    expect(result[1]!.priorYear).toBe(7);
  });

  it("ignores transactions from years other than ref and ref-1", () => {
    const features = [
      ...makeFeatures("信義區", 2022, 4, 5), // two years ago — ignore
      ...makeFeatures("信義區", 2024, 4, 3), // current
    ];
    const result = computeVolumeYoY(features, "信義區", 2024);
    expect(result[1]!.currentYear).toBe(3);
    expect(result[1]!.priorYear).toBe(0);
  });

  it("handles December (Q4) prior-year correctly", () => {
    const features = makeFeatures("信義區", 2023, 12, 5);
    const result = computeVolumeYoY(features, "信義區", 2024);
    expect(result[3]!.priorYear).toBe(5); // Q4 prior
    expect(result[3]!.currentYear).toBe(0);
  });
});

// ── computeVolumeYoY — district filtering ────────────────────────────────────

describe("computeVolumeYoY — district filtering", () => {
  it("excludes transactions from other districts", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 3, 8),
      ...makeFeatures("中山區", 2024, 3, 5), // different district
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[0]!.currentYear).toBe(8);
  });

  it("filters by city when city is provided", () => {
    const features = [
      ...makeFeatures("中正區", 2024, 6, 6, "台北市"),
      ...makeFeatures("中正區", 2024, 6, 4, "基隆市"), // same district name, different city
    ];
    const result = computeVolumeYoY(features, "中正區", 2024, "台北市");
    expect(result[1]!.currentYear).toBe(6);
  });

  it("does not filter by city when city is omitted", () => {
    const features = [
      ...makeFeatures("中正區", 2024, 6, 6, "台北市"),
      ...makeFeatures("中正區", 2024, 6, 4, "基隆市"),
    ];
    const result = computeVolumeYoY(features, "中正區", 2024);
    expect(result[1]!.currentYear).toBe(10);
  });
});

// ── computeVolumeYoY — yoyPct & insufficiency guard ──────────────────────────

describe("computeVolumeYoY — yoyPct and insufficient guard", () => {
  it("computes positive yoyPct correctly", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 1, 10),
      ...makeFeatures("大安區", 2023, 1, 5),
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[0]!.yoyPct).toBeCloseTo(100, 1); // (10-5)/5 * 100
  });

  it("computes negative yoyPct correctly", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 4, 4),
      ...makeFeatures("大安區", 2023, 4, 10),
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[1]!.yoyPct).toBeCloseTo(-60, 1);
  });

  it("returns null yoyPct when priorYear is 0", () => {
    const features = makeFeatures("大安區", 2024, 7, 6);
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[2]!.yoyPct).toBeNull();
  });

  it("marks insufficientCurrent when currentYear < 4", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 1, 3), // 3 < 4 → insufficient
      ...makeFeatures("大安區", 2023, 1, 5),
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[0]!.insufficientCurrent).toBe(true);
  });

  it("marks insufficientPrior when priorYear < 4", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 1, 5),
      ...makeFeatures("大安區", 2023, 1, 2), // 2 < 4 → insufficient
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[0]!.insufficientPrior).toBe(true);
  });

  it("does not mark insufficient when count is exactly 4", () => {
    const features = [
      ...makeFeatures("大安區", 2024, 1, 4),
      ...makeFeatures("大安區", 2023, 1, 4),
    ];
    const result = computeVolumeYoY(features, "大安區", 2024);
    expect(result[0]!.insufficientCurrent).toBe(false);
    expect(result[0]!.insufficientPrior).toBe(false);
  });

  it("returns all zeros for empty feature array", () => {
    const result = computeVolumeYoY([], "大安區", 2024);
    expect(result.every((v) => v.currentYear === 0 && v.priorYear === 0)).toBe(true);
  });
});

// ── computeYTDSummary ─────────────────────────────────────────────────────────

describe("computeYTDSummary", () => {
  const sampleVolumes: QuarterlyVolume[] = [
    { quarter: "Q1", currentYear: 10, priorYear: 8, yoyPct: 25, insufficientCurrent: false, insufficientPrior: false },
    { quarter: "Q2", currentYear: 6,  priorYear: 10, yoyPct: -40, insufficientCurrent: false, insufficientPrior: false },
    { quarter: "Q3", currentYear: 0,  priorYear: 5,  yoyPct: null, insufficientCurrent: true, insufficientPrior: false },
    { quarter: "Q4", currentYear: 0,  priorYear: 4,  yoyPct: null, insufficientCurrent: true, insufficientPrior: false },
  ];

  it("sums currentYear across all quarters", () => {
    expect(computeYTDSummary(sampleVolumes, 12).ytdCurrent).toBe(16);
  });

  it("sums priorYear across all quarters", () => {
    expect(computeYTDSummary(sampleVolumes, 12).ytdPrior).toBe(27);
  });

  it("computes ytdYoYPct correctly", () => {
    // (16 - 27) / 27 * 100 = -40.74... → rounded to -40.7
    const { ytdYoYPct } = computeYTDSummary(sampleVolumes, 12);
    expect(ytdYoYPct).toBeCloseTo(-40.7, 0);
  });

  it("excludes future quarters when currentMonth is in Q2 (month 5)", () => {
    // Only Q1 and Q2 should be included (max quarter = ceil(5/3) = 2)
    const { ytdCurrent, ytdPrior } = computeYTDSummary(sampleVolumes, 5);
    expect(ytdCurrent).toBe(16); // Q1(10) + Q2(6)
    expect(ytdPrior).toBe(18);   // Q1(8) + Q2(10)
  });

  it("only includes Q1 when currentMonth is in Q1 (month 2)", () => {
    const { ytdCurrent, ytdPrior } = computeYTDSummary(sampleVolumes, 2);
    expect(ytdCurrent).toBe(10);
    expect(ytdPrior).toBe(8);
  });

  it("returns null ytdYoYPct when ytdPrior is 0", () => {
    const zeroPrior: QuarterlyVolume[] = [
      { quarter: "Q1", currentYear: 5, priorYear: 0, yoyPct: null, insufficientCurrent: false, insufficientPrior: true },
      { quarter: "Q2", currentYear: 3, priorYear: 0, yoyPct: null, insufficientCurrent: true, insufficientPrior: true },
      { quarter: "Q3", currentYear: 0, priorYear: 0, yoyPct: null, insufficientCurrent: true, insufficientPrior: true },
      { quarter: "Q4", currentYear: 0, priorYear: 0, yoyPct: null, insufficientCurrent: true, insufficientPrior: true },
    ];
    expect(computeYTDSummary(zeroPrior, 12).ytdYoYPct).toBeNull();
  });

  it("handles all-zero input gracefully", () => {
    const zeros: QuarterlyVolume[] = ["Q1", "Q2", "Q3", "Q4"].map((q) => ({
      quarter: q as QuarterlyVolume["quarter"],
      currentYear: 0, priorYear: 0, yoyPct: null,
      insufficientCurrent: true, insufficientPrior: true,
    }));
    const summary = computeYTDSummary(zeros, 12);
    expect(summary.ytdCurrent).toBe(0);
    expect(summary.ytdPrior).toBe(0);
    expect(summary.ytdYoYPct).toBeNull();
  });
});

// ── formatYoYPct ──────────────────────────────────────────────────────────────

describe("formatYoYPct", () => {
  it("formats positive integer pct with + sign", () => {
    expect(formatYoYPct(28)).toBe("+28%");
  });

  it("formats negative pct with - sign", () => {
    expect(formatYoYPct(-32)).toBe("-32%");
  });

  it("formats decimal pct", () => {
    expect(formatYoYPct(12.5)).toBe("+12.5%");
  });

  it("formats 0 as +0%", () => {
    expect(formatYoYPct(0)).toBe("+0%");
  });

  it("returns — for null", () => {
    expect(formatYoYPct(null)).toBe("—");
  });

  it("formats negative decimal pct", () => {
    expect(formatYoYPct(-5.5)).toBe("-5.5%");
  });
});
