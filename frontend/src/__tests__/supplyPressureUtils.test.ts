import { describe, it, expect } from "vitest";
import {
  computeSupplyPressure,
  type DistrictSupplyDemand,
  type SupplyPressureTier,
  TIER_LABEL_ZH,
  MIN_COMPLETION_RECORDS,
} from "../lib/supplyPressureUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeData(overrides: Partial<DistrictSupplyDemand> = {}): DistrictSupplyDemand {
  return {
    district: "大安區",
    city: "台北市",
    upcomingCompletions: 600,
    completionRecordCount: 8,
    populationEstimate: 30_000,
    txCount12mo: 80,
    txCountPrior12mo: 80,
    buyerTimingScore: 50,
    ...overrides,
  };
}

// ── Data-sufficiency gate ─────────────────────────────────────────────────────

describe("data-sufficiency gate (completionRecordCount < MIN_COMPLETION_RECORDS)", () => {
  it("returns insufficient=true when completionRecordCount is 0", () => {
    const result = computeSupplyPressure(makeData({ completionRecordCount: 0 }));
    expect(result.insufficient).toBe(true);
    expect(result.score).toBe(0);
    expect(result.completionsPer1000).toBe(0);
  });

  it("returns insufficient=true when completionRecordCount is 4 (below threshold)", () => {
    const result = computeSupplyPressure(makeData({ completionRecordCount: 4 }));
    expect(result.insufficient).toBe(true);
  });

  it(`returns insufficient=false when completionRecordCount equals MIN_COMPLETION_RECORDS (${MIN_COMPLETION_RECORDS})`, () => {
    const result = computeSupplyPressure(makeData({ completionRecordCount: MIN_COMPLETION_RECORDS }));
    expect(result.insufficient).toBe(false);
  });

  it("returns insufficient=false when completionRecordCount is well above threshold", () => {
    const result = computeSupplyPressure(makeData({ completionRecordCount: 32 }));
    expect(result.insufficient).toBe(false);
  });
});

// ── Zero-value edge cases ─────────────────────────────────────────────────────

describe("zero / missing value graceful fallbacks", () => {
  it("returns low tier with score 0 when upcomingCompletions is 0", () => {
    const result = computeSupplyPressure(makeData({ upcomingCompletions: 0 }));
    expect(result.score).toBe(0);
    expect(result.tier).toBe("low");
    expect(result.insufficient).toBe(false);
  });

  it("uses neutral (1.0) velocity weight when txCountPrior12mo is undefined", () => {
    const result = computeSupplyPressure(makeData({ txCountPrior12mo: undefined }));
    expect(result.velocityWeight).toBe(1.0);
  });

  it("uses neutral velocity weight when txCountPrior12mo is zero", () => {
    const result = computeSupplyPressure(makeData({ txCountPrior12mo: 0 }));
    expect(result.velocityWeight).toBe(1.0);
  });

  it("falls back to default population (30,000) when populationEstimate is 0", () => {
    const with0Pop = computeSupplyPressure(makeData({ populationEstimate: 0 }));
    const withDefault = computeSupplyPressure(makeData({ populationEstimate: undefined }));
    expect(with0Pop.completionsPer1000).toBe(withDefault.completionsPer1000);
  });

  it("treats null buyerTimingScore as neutral (50) — score must match explicit 50", () => {
    const withNull  = computeSupplyPressure(makeData({ buyerTimingScore: null }));
    const withFifty = computeSupplyPressure(makeData({ buyerTimingScore: 50 }));
    expect(withNull.score).toBeCloseTo(withFifty.score, 1);
  });

  it("clamps buyerTimingScore=0 to 1 — no division-by-zero error", () => {
    expect(() => computeSupplyPressure(makeData({ buyerTimingScore: 0 }))).not.toThrow();
    const result = computeSupplyPressure(makeData({ buyerTimingScore: 0 }));
    expect(result.score).toBeGreaterThan(0);
    expect(result.score).toBeLessThanOrEqual(100);
  });

  it("returns falling velocity weight (1.3) when txCount12mo is 0 and txCountPrior12mo > 0", () => {
    const result = computeSupplyPressure(makeData({ txCount12mo: 0, txCountPrior12mo: 50 }));
    expect(result.velocityWeight).toBe(1.3);
  });
});

// ── Velocity weight ───────────────────────────────────────────────────────────

describe("velocity weight derivation", () => {
  it("applies rising weight (0.7) when current txns > 1.2× prior", () => {
    const result = computeSupplyPressure(makeData({ txCount12mo: 130, txCountPrior12mo: 100 }));
    expect(result.velocityWeight).toBe(0.7);
  });

  it("applies flat weight (1.0) when ratio is between 0.8 and 1.2", () => {
    const result = computeSupplyPressure(makeData({ txCount12mo: 100, txCountPrior12mo: 100 }));
    expect(result.velocityWeight).toBe(1.0);
  });

  it("applies falling weight (1.3) when current txns < 0.8× prior", () => {
    const result = computeSupplyPressure(makeData({ txCount12mo: 60, txCountPrior12mo: 100 }));
    expect(result.velocityWeight).toBe(1.3);
  });

  it("rising velocity produces lower score than falling velocity (all else equal)", () => {
    const rising  = computeSupplyPressure(makeData({ txCount12mo: 130, txCountPrior12mo: 100 }));
    const falling = computeSupplyPressure(makeData({ txCount12mo: 60,  txCountPrior12mo: 100 }));
    expect(rising.score).toBeLessThan(falling.score);
  });
});

// ── Tier boundaries ───────────────────────────────────────────────────────────

describe("tier assignment", () => {
  const ALL_TIERS: SupplyPressureTier[] = ["low", "medium-low", "medium", "medium-high", "high"];

  it("TIER_LABEL_ZH contains zh-TW labels for all 5 tiers", () => {
    for (const tier of ALL_TIERS) {
      expect(TIER_LABEL_ZH[tier]).toBeTruthy();
      expect(typeof TIER_LABEL_ZH[tier]).toBe("string");
    }
    expect(TIER_LABEL_ZH["low"]).toBe("低");
    expect(TIER_LABEL_ZH["medium-high"]).toBe("中高");
    expect(TIER_LABEL_ZH["high"]).toBe("高");
  });

  it("result tierLabel matches TIER_LABEL_ZH[tier]", () => {
    const result = computeSupplyPressure(makeData());
    expect(result.tierLabel).toBe(TIER_LABEL_ZH[result.tier]);
  });

  it("produces 'low' tier with very low completions and high buyer score (cold market)", () => {
    const result = computeSupplyPressure(makeData({
      upcomingCompletions: 50,
      buyerTimingScore: 90,
      txCount12mo: 120,
      txCountPrior12mo: 100,
    }));
    expect(result.tier).toBe("low");
  });

  it("produces 'high' tier with high completions, low buyer score, and falling velocity", () => {
    const result = computeSupplyPressure(makeData({
      upcomingCompletions: 3000,
      buyerTimingScore: 5,
      txCount12mo: 40,
      txCountPrior12mo: 100,
      populationEstimate: 30_000,
    }));
    expect(result.tier).toBe("high");
    expect(result.score).toBeGreaterThanOrEqual(80);
  });

  it("score is always between 0 and 100 (inclusive)", () => {
    const extremeHigh = computeSupplyPressure(makeData({
      upcomingCompletions: 100_000,
      buyerTimingScore: 1,
      txCount12mo: 1,
      txCountPrior12mo: 1000,
    }));
    expect(extremeHigh.score).toBeGreaterThanOrEqual(0);
    expect(extremeHigh.score).toBeLessThanOrEqual(100);

    const extremeLow = computeSupplyPressure(makeData({
      upcomingCompletions: 0,
      buyerTimingScore: 100,
      txCount12mo: 1000,
      txCountPrior12mo: 100,
    }));
    expect(extremeLow.score).toBe(0);
  });

  it("higher completions produce higher scores (all else equal)", () => {
    const low  = computeSupplyPressure(makeData({ upcomingCompletions: 200 }));
    const high = computeSupplyPressure(makeData({ upcomingCompletions: 2000 }));
    expect(high.score).toBeGreaterThan(low.score);
  });

  it("lower buyer timing score produces higher pressure score (same completions)", () => {
    const hotMarket  = computeSupplyPressure(makeData({ buyerTimingScore: 10 }));
    const coldMarket = computeSupplyPressure(makeData({ buyerTimingScore: 90 }));
    expect(hotMarket.score).toBeGreaterThan(coldMarket.score);
  });

  it("larger population reduces completionsPer1000 (same unit count)", () => {
    const small = computeSupplyPressure(makeData({ populationEstimate: 10_000 }));
    const large = computeSupplyPressure(makeData({ populationEstimate: 100_000 }));
    expect(small.completionsPer1000).toBeGreaterThan(large.completionsPer1000);
    expect(small.score).toBeGreaterThan(large.score);
  });
});

// ── Full-width digit normalization reminder ───────────────────────────────────
// (The util itself receives pre-parsed numbers; this test documents the contract
//  that callers must normalize gov data strings with .normalize('NFKC') before parsing.)

describe("normalization contract", () => {
  it("accepts numeric fields directly — no string normalization needed inside util", () => {
    const result = computeSupplyPressure(makeData({ upcomingCompletions: 500, txCount12mo: 75 }));
    expect(result.insufficient).toBe(false);
    expect(typeof result.score).toBe("number");
  });
});
