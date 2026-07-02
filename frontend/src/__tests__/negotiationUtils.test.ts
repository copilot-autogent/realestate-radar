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
