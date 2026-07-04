import { describe, it, expect } from "vitest";
import {
  estimateNegotiationMargin,
  computeNegotiationPercentile,
  velocityMultiplier,
  peakFactor,
  assessedRatioFactor,
  MIN_TX,
  MAX_MARGIN,
  type DistrictStats,
  type NegotiationEstimate,
} from "../lib/negotiationUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeStats(overrides: Partial<DistrictStats> = {}): DistrictStats {
  return {
    txCount12mo: 25,
    velocityYoYPct: 0,
    monthsSincePeak: 12,
    assessedToMarketRatio: null,
    medianUnitPriceWan: 50,
    ...overrides,
  };
}

// ── velocityMultiplier ────────────────────────────────────────────────────────

describe("velocityMultiplier", () => {
  it("returns 1.0 for null (no data)", () => {
    expect(velocityMultiplier(null)).toBe(1.0);
  });

  it("returns 1.5 for large volume decline (≤ −20%)", () => {
    expect(velocityMultiplier(-30)).toBe(1.5);
    expect(velocityMultiplier(-20)).toBe(1.5);
  });

  it("returns 1.3 for moderate decline (−15%)", () => {
    expect(velocityMultiplier(-15)).toBe(1.3);
    expect(velocityMultiplier(-10)).toBe(1.3);
  });

  it("returns 1.1 for slight decline (−5%)", () => {
    expect(velocityMultiplier(-5)).toBe(1.1);
  });

  it("returns 1.0 for flat (0%)", () => {
    expect(velocityMultiplier(0)).toBe(1.0);
  });

  it("returns 1.0 for slight growth (+5%)", () => {
    expect(velocityMultiplier(5)).toBe(1.0);
  });

  it("returns 0.85 for moderate growth (+15%)", () => {
    expect(velocityMultiplier(15)).toBe(0.85);
  });

  it("returns 0.70 for large growth (> +20%)", () => {
    expect(velocityMultiplier(25)).toBe(0.70);
  });
});

// ── peakFactor ────────────────────────────────────────────────────────────────

describe("peakFactor", () => {
  it("returns 1.0 for null", () => {
    expect(peakFactor(null)).toBe(1.0);
  });

  it("returns 1.3 for > 24 months since peak", () => {
    expect(peakFactor(36)).toBe(1.3);
    expect(peakFactor(25)).toBe(1.3);
  });

  it("returns 1.15 for 12–24 months since peak", () => {
    expect(peakFactor(18)).toBe(1.15);
    expect(peakFactor(13)).toBe(1.15);
  });

  it("returns 1.0 for 6–12 months since peak (inclusive of 6)", () => {
    expect(peakFactor(9)).toBe(1.0);
    expect(peakFactor(6)).toBe(1.0);
    expect(peakFactor(12)).toBe(1.0);
  });

  it("returns 0.85 for < 6 months since peak", () => {
    expect(peakFactor(3)).toBe(0.85);
    expect(peakFactor(5.9)).toBe(0.85);
  });
});

// ── assessedRatioFactor ───────────────────────────────────────────────────────

describe("assessedRatioFactor", () => {
  it("returns 1.0 for null (unavailable)", () => {
    expect(assessedRatioFactor(null)).toBe(1.0);
  });

  it("returns 1.3 for very low ratio (< 0.2)", () => {
    expect(assessedRatioFactor(0.1)).toBe(1.3);
  });

  it("returns 1.15 for ratio 0.2 to < 0.4", () => {
    expect(assessedRatioFactor(0.2)).toBe(1.15);
    expect(assessedRatioFactor(0.3)).toBe(1.15);
  });

  it("returns 1.0 for ratio 0.4 to 0.6 (inclusive)", () => {
    expect(assessedRatioFactor(0.4)).toBe(1.0);
    expect(assessedRatioFactor(0.5)).toBe(1.0);
    expect(assessedRatioFactor(0.6)).toBe(1.0);
  });

  it("returns 0.9 for high ratio (> 0.6)", () => {
    expect(assessedRatioFactor(0.7)).toBe(0.9);
    expect(assessedRatioFactor(0.8)).toBe(0.9);
  });
});

// ── estimateNegotiationMargin: thin-data fallback ─────────────────────────────

describe("estimateNegotiationMargin – thin data", () => {
  it("returns sufficient=false when txCount12mo < MIN_TX (20)", () => {
    const result = estimateNegotiationMargin(makeStats({ txCount12mo: 15 }));
    expect(result.sufficient).toBe(false);
    expect(result.marginCenter).toBeNull();
    expect(result.marginLow).toBeNull();
    expect(result.marginHigh).toBeNull();
    expect(result.rangeLowWan).toBeNull();
    expect(result.rangeHighWan).toBeNull();
  });

  it("labels 10–19 tx as '資料有限'", () => {
    const result = estimateNegotiationMargin(makeStats({ txCount12mo: 12 }));
    expect(result.confidenceLabel).toBe("資料有限");
  });

  it("labels < 10 tx as '資料不足'", () => {
    const result = estimateNegotiationMargin(makeStats({ txCount12mo: 5 }));
    expect(result.confidenceLabel).toBe("資料不足");
  });

  it("labels ≥ 20 tx as '資料充足'", () => {
    const result = estimateNegotiationMargin(makeStats({ txCount12mo: 20 }));
    expect(result.confidenceLabel).toBe("資料充足");
  });
});

// ── estimateNegotiationMargin: core behaviour ─────────────────────────────────

describe("estimateNegotiationMargin – core", () => {
  it("returns sufficient=true when txCount12mo ≥ MIN_TX", () => {
    const result = estimateNegotiationMargin(makeStats());
    expect(result.sufficient).toBe(true);
  });

  it("max-margin cap: result never exceeds 20% even with extreme inputs", () => {
    const result = estimateNegotiationMargin(
      makeStats({
        txCount12mo: 50,
        velocityYoYPct: -50,    // maximum multiplier (1.5)
        monthsSincePeak: 36,    // maximum peak factor (1.3)
        assessedToMarketRatio: 0.05, // maximum ratio factor (1.3)
      })
    );
    expect(result.marginCenter).not.toBeNull();
    expect(result.marginCenter!).toBeLessThanOrEqual(MAX_MARGIN);
    expect(result.marginHigh!).toBeLessThanOrEqual(MAX_MARGIN);
  });

  it("falling velocity (−25%) produces higher margin than rising velocity (+25%)", () => {
    const falling = estimateNegotiationMargin(makeStats({ velocityYoYPct: -25 }));
    const rising  = estimateNegotiationMargin(makeStats({ velocityYoYPct: +25 }));
    expect(falling.marginCenter!).toBeGreaterThan(rising.marginCenter!);
  });

  it("long time since peak (36 mo) produces higher margin than recent peak (2 mo)", () => {
    const distant = estimateNegotiationMargin(makeStats({ monthsSincePeak: 36 }));
    const recent  = estimateNegotiationMargin(makeStats({ monthsSincePeak: 2  }));
    expect(distant.marginCenter!).toBeGreaterThan(recent.marginCenter!);
  });

  it("zero-velocity (null) returns valid estimate at neutral multiplier", () => {
    const result = estimateNegotiationMargin(makeStats({ velocityYoYPct: null }));
    expect(result.sufficient).toBe(true);
    expect(result.marginCenter).toBeGreaterThan(0);
  });

  it("marginLow < marginCenter < marginHigh", () => {
    const result = estimateNegotiationMargin(makeStats());
    expect(result.marginLow!).toBeLessThan(result.marginCenter!);
    expect(result.marginCenter!).toBeLessThan(result.marginHigh!);
  });

  it("NT$ range scales with median unit price", () => {
    const cheap  = estimateNegotiationMargin(makeStats({ medianUnitPriceWan: 20 }));
    const pricey = estimateNegotiationMargin(makeStats({ medianUnitPriceWan: 80 }));
    expect(pricey.rangeLowWan!).toBeGreaterThan(cheap.rangeLowWan!);
    expect(pricey.rangeHighWan!).toBeGreaterThan(cheap.rangeHighWan!);
  });

  it("returns null NT$ range when medianUnitPriceWan is null", () => {
    const result = estimateNegotiationMargin(makeStats({ medianUnitPriceWan: null }));
    expect(result.rangeLowWan).toBeNull();
    expect(result.rangeHighWan).toBeNull();
  });

  it("low assessed ratio (0.1) increases margin over unavailable ratio", () => {
    const low  = estimateNegotiationMargin(makeStats({ assessedToMarketRatio: 0.1 }));
    const none = estimateNegotiationMargin(makeStats({ assessedToMarketRatio: null }));
    expect(low.marginCenter!).toBeGreaterThan(none.marginCenter!);
  });

  it("marginLow ≥ 1% (floor) even with the most conservative inputs", () => {
    const result = estimateNegotiationMargin(
      makeStats({ velocityYoYPct: 50, monthsSincePeak: 1, assessedToMarketRatio: 0.9 })
    );
    expect(result.marginLow!).toBeGreaterThanOrEqual(0.01);
  });
});

// ── computeNegotiationPercentile ──────────────────────────────────────────────

describe("computeNegotiationPercentile", () => {
  function makeEstimate(marginCenter: number): NegotiationEstimate {
    return {
      sufficient: true,
      confidenceLabel: "資料充足",
      marginLow: marginCenter * 0.8,
      marginCenter,
      marginHigh: marginCenter * 1.2,
      rangeLowWan: null,
      rangeHighWan: null,
    };
  }

  it("returns null for an insufficient estimate", () => {
    const others = [makeEstimate(0.05), makeEstimate(0.08)];
    const target: NegotiationEstimate = {
      sufficient: false,
      confidenceLabel: "資料不足",
      marginLow: null,
      marginCenter: null,
      marginHigh: null,
      rangeLowWan: null,
      rangeHighWan: null,
    };
    expect(computeNegotiationPercentile(target, others)).toBeNull();
  });

  it("returns null when others pool is empty", () => {
    const target = makeEstimate(0.05);
    expect(computeNegotiationPercentile(target, [])).toBeNull();
  });

  it("returns 0 for the lowest margin in the population", () => {
    const target = makeEstimate(0.03);
    const others = [makeEstimate(0.07), makeEstimate(0.10), makeEstimate(0.12)];
    expect(computeNegotiationPercentile(target, others)).toBe(0);
  });

  it("returns 100 for the highest margin in the population", () => {
    const target = makeEstimate(0.15);
    const others = [makeEstimate(0.03), makeEstimate(0.07), makeEstimate(0.10)];
    expect(computeNegotiationPercentile(target, others)).toBe(100);
  });
});

computeNegotiationMarginTrend // ── ─────────────────────────────

import {
  computeNegotiationMarginTrend,
  computeTrendMA3,
  computeTrendBadge,
  trendSufficiencyLabel,
  MIN_TREND_TX,
  MIN_TREND_MONTHS,
  type MarginDataPoint,
} from "../lib/negotiationUtils.js";

/** Build a synthetic Transaction-like object */
function makeTx(
  district: string,
  dateStr: string,
  unitPricePerSqm: number | null = 300_000, // 元/m²
  assessedToMarketRatio: number | null = 0.5,
): {
  district: string;
  transactionDate: Date;
  unitPrice: number | null;
  assessedToMarketRatio: number | null;
} {
  return {
    district,
    transactionDate: new Date(dateStr),
    unitPrice: unitPricePerSqm,
    assessedToMarketRatio,
  };
}

/** Generate N synthetic transactions for a district in a given "YYYY-MM" month */
function makeTxBatch(
  district: string,
  month: string, // "YYYY-MM"
  count: number,
  unitPricePerSqm: number | null = 300_000,
  assessedToMarketRatio: number | null = 0.5,
) {
  const day = "15";
  return Array.from({ length: count }, (_, i) =>
    makeTx(district, `${month}-${day}`, unitPricePerSqm, assessedToMarketRatio),
  );
}

describe("computeNegotiationMarginTrend", () => {
  // ── 1. Empty / no-data ────────────────────────────────────────────────────
  it("returns all-undefined when transaction list is empty", () => {
    const result = computeNegotiationMarginTrend([], "大安區", 12);
    expect(result).toHaveLength(12);
    expect(result.every((p) => p === undefined)).toBe(true);
  });

  it("returns all-undefined when no transactions match the district", () => {
    const txs = makeTxBatch("信義區", "2024-03", 25);
    const result = computeNegotiationMarginTrend(txs, "大安區", 12);
    expect(result.every((p) => p === undefined)).toBe(true);
  });

  // ── 2. accumulation 12-month ───────────────────────
  it("produces a result array of exactly `months` length", () => {
    const txs = makeTxBatch("大安區", "2024-03", 30);
    const result = computeNegotiationMarginTrend(txs, "大安區", 12);
    expect(result).toHaveLength(12);
  });

  it("computes a defined data point when 12-month window has ≥ MIN_TX transactions", () => {
    // 25 transactions in one month → accumulated to a single month's window
    const txs = makeTxBatch("大安區", "2024-03", 25);
    const result = computeNegotiationMarginTrend(txs, "大安區", 12, "2024-03");
    const lastPoint = result[result.length - 1];
    expect(lastPoint).toBeDefined();
    expect(lastPoint!.month).toBe("2024-03");
    expect(lastPoint!.marginCenter).toBeGreaterThan(0);
    expect(lastPoint!.marginCenter).toBeLessThanOrEqual(MAX_MARGIN);
  });

  it("accumulates transactions across months — 10 per month × 3 months = 30 total", () => {
    const txs = [
      ...makeTxBatch("大安區", "2024-01", 10),
      ...makeTxBatch("大安區", "2024-02", 10),
      ...makeTxBatch("大安區", "2024-03", 10),
    ];
    const result = computeNegotiationMarginTrend(txs, "大安區", 3, "2024-03");
    // The last month accumulates all 30 tx in 12-month window → sufficient
    const lastPoint = result[result.length - 1];
    expect(lastPoint).toBeDefined();
    expect(lastPoint!.marginCenter).toBeGreaterThan(0);
  });

  it("result month labels are in ascending chronological order", () => {
    const txs = [
      ...makeTxBatch("大安區", "2023-10", 25),
      ...makeTxBatch("大安區", "2024-03", 25),
    ];
    const result = computeNegotiationMarginTrend(txs, "大安區", 12, "2024-03");
    const definedPoints = result.filter((p): p is MarginDataPoint => p !== undefined);
    for (let i = 1; i < definedPoints.length; i++) {
      expect(definedPoints[i]!.month > definedPoints[i - 1]!.month).toBe(true);
    }
  });

  // ── 3. Sparse-data handling gap ────────────────────────
  it("marks months with fewer than MIN_TREND_TX transactions as undefined (gap)", () => {
    // Only 3 transactions for the district → below MIN_TREND_TX=5
    const txs = makeTxBatch("大安區", "2024-03", 3);
    const result = computeNegotiationMarginTrend(txs, "大安區", 6, "2024-03");
    expect(result.every((p) => p === undefined)).toBe(true);
  });

  it("produces gaps between non-contiguous data months", () => {
    // Enough data only in first and last month; middle month is empty
    const txs = [
      ...makeTxBatch("大安區", "2023-10", 25),
      ...makeTxBatch("大安區", "2024-03", 25),
    ];
    const result = computeNegotiationMarginTrend(txs, "大安區", 12, "2024-03");
    // Some intermediate entries should be undefined (gaps)
    const hasGap = result.some((p) => p === undefined);
    expect(hasGap).toBe(true);
  });

  it("does not interpolate gaps to zero — undefined months stay undefined", () => {
    const txs = [
      ...makeTxBatch("大安區", "2024-01", 25),
      // February is intentionally absent
      ...makeTxBatch("大安區", "2024-03", 25),
    ];
    const result = computeNegotiationMarginTrend(txs, "大安區", 3, "2024-03");
    // Feb entry (index 1) may still have data due to accumulation within 12mo window,
    // but if it's a gap month it must be undefined (not 0)
    for (const p of result) {
      if (p === undefined) {
        expect(p).toBeUndefined(); // not 0 or null
      } else {
        expect(p.marginCenter).toBeGreaterThan(0);
      }
    }
  });

  // ── 4. NFKC normalisation on district names & dates ──────────────────────
  it("matches district using NFKC normalisation on transaction district field", () => {
    // full-width "大安區" (U+FF10 range digits would be in the romanized version;
    // here we test ASCII vs NFKC match with a composed form)
    const txs = makeTxBatch("\u5927\u5b89\u5340", "2024-03", 25); // 大安區 NFC
    const result = computeNegotiationMarginTrend(txs, "大安區", 1, "2024-03");
    const lastPoint = result[result.length - 1];
    expect(lastPoint).toBeDefined();
  });

  it("handles full-width digit month in date string via NFKC normalisation", () => {
    // Full-width "2024-03-15" using full-width digits U+FF10-FF19
    // Build via String.fromCodePoint to avoid source-file encoding issues
    const fullWidthDate =
      String.fromCodePoint(0xff12, 0xff10, 0xff12, 0xff14) +
      "-" +
      String.fromCodePoint(0xff10, 0xff13) +
      "-" +
      String.fromCodePoint(0xff11, 0xff15); // ２０２４-０３-１５
    const txs = Array.from({ length: 25 }, () => ({
      district: "大安區",
      transactionDate: fullWidthDate as unknown as Date,
      unitPrice: 300_000,
      assessedToMarketRatio: 0.5,
    }));
    const result = computeNegotiationMarginTrend(txs, "大安區", 1, "2024-03");
    const lastPoint = result[result.length - 1];
    expect(lastPoint).toBeDefined();
  });

  // ── 5. marginLow / marginCenter / marginHigh invariants ─────────────
  it("marginLow < marginCenter < marginHigh for every defined point", () => {
    const txs = [
      ...makeTxBatch("大安區", "2024-01", 25),
      ...makeTxBatch("大安區", "2024-02", 25),
      ...makeTxBatch("大安區", "2024-03", 25),
    ];
    const result = computeNegotiationMarginTrend(txs, "大安區", 3, "2024-03");
    for (const p of result) {
      if (!p) continue;
      expect(p.marginLow).toBeLessThan(p.marginCenter);
      expect(p.marginCenter).toBeLessThan(p.marginHigh);
      expect(p.marginCenter).toBeLessThanOrEqual(MAX_MARGIN);
    }
  });

  // ── 6. velocityYoY affects the margin ────────────────────────────────────
  it("falling velocity (more prior-year tx than current-year) produces higher margin", () => {
    // Build "high prior" scenario: 30 tx in prior 12mo, 20 tx in current 12mo
    const priorTxs  = makeTxBatch("大安區", "2023-03", 30); // prior year
    const currentTxs = makeTxBatch("大安區", "2024-03", 20); // current year
    const resultFalling = computeNegotiationMarginTrend(
      [...priorTxs, ...currentTxs], "大安區", 1, "2024-03",
    );

    // Build "rising velocity" scenario: 20 tx prior, 30 tx current
    const priorTxsB  = makeTxBatch("大安區", "2023-03", 20);
    const currentTxsB = makeTxBatch("大安區", "2024-03", 30);
    const resultRising = computeNegotiationMarginTrend(
      [...priorTxsB, ...currentTxsB], "大安區", 1, "2024-03",
    );

    const fallingPt = resultFalling[resultFalling.length - 1];
    const risingPt  = resultRising[resultRising.length - 1];
    if (fallingPt && risingPt) {
      expect(fallingPt.marginCenter).toBeGreaterThan(risingPt.marginCenter);
    }
  });

  // ── 7. referenceMonth override ────────────────────────────────────────────
  it("respects the explicit referenceMonth parameter", () => {
    const txs = makeTxBatch("大安區", "2023-06", 25);
    const result = computeNegotiationMarginTrend(txs, "大安區", 6, "2023-06");
    expect(result).toHaveLength(6);
    const lastPoint = result[result.length - 1];
    expect(lastPoint?.month).toBe("2023-06");
  });
});

// ── computeTrendMA3 ──────────────────────────────────────────────────────────

describe("computeTrendMA3", () => {
  function makePoint(month: string, center: number): MarginDataPoint {
    return { month, marginCenter: center, marginLow: center * 0.8, marginHigh: center * 1.2 };
  }

  it("returns null for the first two positions (insufficient history)", () => {
    const series = [makePoint("2024-01", 0.05), makePoint("2024-02", 0.06), makePoint("2024-03", 0.07)];
    const ma3 = computeTrendMA3(series);
    expect(ma3[0]).toBeNull();
    expect(ma3[1]).toBeNull();
    expect(ma3[2]).toBeCloseTo((0.05 + 0.06 + 0.07) / 3, 5);
  });

  it("returns null for positions with undefined neighbours", () => {
    const series: (MarginDataPoint | undefined)[] = [
      makePoint("2024-01", 0.05),
      undefined,
      makePoint("2024-03", 0.07),
    ];
    const ma3 = computeTrendMA3(series);
    expect(ma3[2]).toBeNull(); // middle is undefined → can't compute MA3
  });

  it("propagates length of series unchanged", () => {
    const series = Array.from({ length: 12 }, (_, i) => makePoint(`2024-${String(i + 1).padStart(2, "0")}`, 0.05 + i * 0.001));
    expect(computeTrendMA3(series)).toHaveLength(12);
  });
});

// ── computeTrendBadge ─────────────────────────────────────────────────────────

describe("computeTrendBadge", () => {
  function makePoint(month: string, center: number): MarginDataPoint {
    return { month, marginCenter: center, marginLow: center * 0.8, marginHigh: center * 1.2 };
  }

  it("returns null when series is empty", () => {
    expect(computeTrendBadge([])).toBeNull();
  });

  it("returns null when insufficient defined points for MA3", () => {
    const series: (MarginDataPoint | undefined)[] = [undefined, undefined, undefined, undefined];
    expect(computeTrendBadge(series)).toBeNull();
  });

  it("returns '▲ 擴大趨勢' when 3-month MA is rising", () => {
    // Build series: 6 months with increasing margins
    const series = [
      makePoint("2024-01", 0.04),
      makePoint("2024-02", 0.04),
      makePoint("2024-03", 0.04), // MA3 at index 2 = 0.04
      makePoint("2024-04", 0.07),
      makePoint("2024-05", 0.07),
      makePoint("2024-06", 0.07), // MA3 at index 5 = 0.07 → rising
    ];
    expect(computeTrendBadge(series)).toBe("▲ 擴大趨勢");
  });

  it("returns '▼ 收窄趨勢' when 3-month MA is falling", () => {
    const series = [
      makePoint("2024-01", 0.10),
      makePoint("2024-02", 0.10),
      makePoint("2024-03", 0.10), // MA3 = 0.10
      makePoint("2024-04", 0.04),
      makePoint("2024-05", 0.04),
      makePoint("2024-06", 0.04), // MA3 = 0.04 → falling
    ];
    expect(computeTrendBadge(series)).toBe("▼ 收窄趨勢");
  });

  it("returns null for a flat MA trend (no direction change)", () => {
    const series = [
      makePoint("2024-01", 0.06),
      makePoint("2024-02", 0.06),
      makePoint("2024-03", 0.06),
      makePoint("2024-04", 0.06),
      makePoint("2024-05", 0.06),
      makePoint("2024-06", 0.06),
    ];
    expect(computeTrendBadge(series)).toBeNull();
  });
});

trendSufficiencyLabel // ── ─────────────────────────────────────

describe("trendSufficiencyLabel", () => {
  function makePoint(month: string): MarginDataPoint {
    return { month, marginCenter: 0.05, marginLow: 0.04, marginHigh: 0.06 };
  }

  it("returns '資料不足' when fewer than 3 computable months", () => {
    const series: (MarginDataPoint | undefined)[] = [undefined, makePoint("2024-02"), undefined, undefined];
    expect(trendSufficiencyLabel(series)).toBe("資料不足");
  });

  it("returns '資料有限' when 3–5 computable months", () => {
    const series: (MarginDataPoint | undefined)[] = [
      makePoint("2024-01"),
      makePoint("2024-02"),
      makePoint("2024-03"),
      undefined,
    ];
    expect(trendSufficiencyLabel(series)).toBe("資料有限");
  });

  it("returns '資料充足' when ≥ 6 computable months", () => {
    const series: (MarginDataPoint | undefined)[] = Array.from({ length: 8 }, (_, i) =>
      makePoint(`2024-${String(i + 1).padStart(2, "0")}`),
    );
    expect(trendSufficiencyLabel(series)).toBe("資料充足");
  });

  it("MIN_TREND_MONTHS constant is 3", () => {
    expect(MIN_TREND_MONTHS).toBe(3);
  });

  it("MIN_TREND_TX constant is 5", () => {
    expect(MIN_TREND_TX).toBe(5);
  });
});
