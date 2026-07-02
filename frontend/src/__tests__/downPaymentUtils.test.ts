import { describe, it, expect } from "vitest";
import {
  computeDownPaymentNTD,
  computeMonthsToGoal,
  monthsToColor,
  computeMonthlyMortgagePayment,
  computeAffordability,
  buildTiers,
  computeTimeline,
  suggestTotalPriceWan,
  arrayMedian,
  formatMonths,
  type DownPaymentPct,
} from "../lib/downPaymentUtils.js";

// ── computeDownPaymentNTD ─────────────────────────────────────────────────────

describe("computeDownPaymentNTD", () => {
  it("5% of 1000萬 = NT$500,000", () => {
    expect(computeDownPaymentNTD(1000, 5)).toBeCloseTo(500_000);
  });

  it("10% of 1000萬 = NT$1,000,000", () => {
    expect(computeDownPaymentNTD(1000, 10)).toBe(1_000_000);
  });

  it("20% of 1000萬 = NT$2,000,000", () => {
    expect(computeDownPaymentNTD(1000, 20)).toBe(2_000_000);
  });

  it("returns 0 for zero price", () => {
    expect(computeDownPaymentNTD(0, 20)).toBe(0);
  });

  it("returns 0 for negative price", () => {
    expect(computeDownPaymentNTD(-100, 10)).toBe(0);
  });

  it("returns 0 for NaN price", () => {
    expect(computeDownPaymentNTD(NaN, 10)).toBe(0);
  });

  it("handles fractional wan values", () => {
    expect(computeDownPaymentNTD(500.5, 10)).toBeCloseTo(500_500);
  });
});

// ── computeMonthsToGoal ───────────────────────────────────────────────────────

describe("computeMonthsToGoal", () => {
  it("returns 0 when current savings already meet or exceed target", () => {
    expect(computeMonthsToGoal(1_000_000, 50_000, 800_000)).toBe(0);
    expect(computeMonthsToGoal(1_000_000, 50_000, 1_000_000)).toBe(0);
  });

  it("returns Infinity when monthly savings is 0", () => {
    expect(computeMonthsToGoal(0, 0, 500_000)).toBe(Infinity);
  });

  it("returns Infinity when monthly savings is negative", () => {
    expect(computeMonthsToGoal(100_000, -1_000, 500_000)).toBe(Infinity);
  });

  it("returns 0 for non-positive target", () => {
    expect(computeMonthsToGoal(0, 50_000, 0)).toBe(0);
    expect(computeMonthsToGoal(0, 50_000, -1)).toBe(0);
  });

  it("returns 0 for NaN target", () => {
    expect(computeMonthsToGoal(0, 50_000, NaN)).toBe(0);
  });

  it("returns Infinity for NaN current savings", () => {
    expect(computeMonthsToGoal(NaN, 50_000, 100_000)).toBe(Infinity);
  });

  it("returns Infinity for NaN monthly savings", () => {
    expect(computeMonthsToGoal(0, NaN, 100_000)).toBe(Infinity);
  });

  it("rounds up to whole months (ceiling)", () => {
    // Gap of 100,001 at 50,000/month → 2.00002 months → ceil = 3
    expect(computeMonthsToGoal(0, 50_000, 100_001)).toBe(3);
  });

  it("exactly fills in whole months (no ceiling)", () => {
    // Gap of 100,000 at 50,000/month → 2 months exactly
    expect(computeMonthsToGoal(0, 50_000, 100_000)).toBe(2);
  });

  it("typical scenario: 500k savings, 30k/month, 1.5M target → 34 months", () => {
    // gap = 1,000,000; 1,000,000 / 30,000 = 33.33 → ceil = 34
    expect(computeMonthsToGoal(500_000, 30_000, 1_500_000)).toBe(34);
  });
});

// ── monthsToColor ─────────────────────────────────────────────────────────────

describe("monthsToColor", () => {
  it("0 months (already achieved) → green", () => {
    expect(monthsToColor(0)).toBe("green");
  });

  it("24 months → green (boundary inclusive)", () => {
    expect(monthsToColor(24)).toBe("green");
  });

  it("25 months → amber", () => {
    expect(monthsToColor(25)).toBe("amber");
  });

  it("48 months → amber (boundary inclusive)", () => {
    expect(monthsToColor(48)).toBe("amber");
  });

  it("49 months → red", () => {
    expect(monthsToColor(49)).toBe("red");
  });

  it("Infinity → red", () => {
    expect(monthsToColor(Infinity)).toBe("red");
  });
});

// ── computeMonthlyMortgagePayment ─────────────────────────────────────────────

describe("computeMonthlyMortgagePayment", () => {
  it("reasonable payment for 800萬 loan at 2.5%, 30yr", () => {
    const payment = computeMonthlyMortgagePayment(800, 2.5, 30);
    // Expected roughly 31,600 NT$/month (standard amortization)
    expect(payment).toBeGreaterThan(28_000);
    expect(payment).toBeLessThan(40_000);
  });

  it("returns 0 for zero loan", () => {
    expect(computeMonthlyMortgagePayment(0, 2.5, 30)).toBe(0);
  });

  it("returns 0 for negative loan", () => {
    expect(computeMonthlyMortgagePayment(-100, 2.5, 30)).toBe(0);
  });

  it("returns 0 for zero or negative years", () => {
    expect(computeMonthlyMortgagePayment(800, 2.5, 0)).toBe(0);
    expect(computeMonthlyMortgagePayment(800, 2.5, -1)).toBe(0);
  });

  it("zero rate falls back to simple principal division", () => {
    // 360萬 over 30 years (360 months) = 10,000/month
    const payment = computeMonthlyMortgagePayment(360, 0, 30);
    expect(payment).toBeCloseTo(10_000, 0);
  });

  it("higher rate → higher payment", () => {
    const low = computeMonthlyMortgagePayment(800, 2.0, 30);
    const high = computeMonthlyMortgagePayment(800, 4.0, 30);
    expect(high).toBeGreaterThan(low);
  });

  it("shorter term → higher payment", () => {
    const long = computeMonthlyMortgagePayment(800, 2.5, 30);
    const short = computeMonthlyMortgagePayment(800, 2.5, 20);
    expect(short).toBeGreaterThan(long);
  });

  it("matches manual calculation for 1000萬 loan, 2.5%, 30yr", () => {
    // P = 10,000,000; r = 2.5/100/12 = 0.002083; n = 360
    const P = 10_000_000;
    const r = 2.5 / 100 / 12;
    const n = 360;
    const expected = P * (r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);
    expect(computeMonthlyMortgagePayment(1000, 2.5, 30)).toBeCloseTo(expected, 0);
  });
});

// ── computeAffordability ─────────────────────────────────────────────────────

describe("computeAffordability", () => {
  it("returns null when income is 0 (skip check)", () => {
    expect(computeAffordability(1000, 20, 2.5, 30, 0)).toBeNull();
  });

  it("returns null for non-positive price", () => {
    expect(computeAffordability(0, 20, 2.5, 30, 80_000)).toBeNull();
    expect(computeAffordability(-100, 20, 2.5, 30, 80_000)).toBeNull();
  });

  it("overCliff=true when payment > 33% of income", () => {
    // 1000萬 at 2.5%, 30yr, 20% down → loan = 800萬 → ~31,600/month
    // income 50,000 → 31,600/50,000 = 63% > 33%
    const result = computeAffordability(1000, 20, 2.5, 30, 50_000);
    expect(result).not.toBeNull();
    expect(result!.overCliff).toBe(true);
    expect(result!.ratio).toBeGreaterThan(0.33);
  });

  it("overCliff=false when payment ≤ 33% of income", () => {
    // 1000萬 at 2.5%, 30yr, 20% down → ~31,600/month
    // income 200,000 → 31,600/200,000 = 15.8% < 33%
    const result = computeAffordability(1000, 20, 2.5, 30, 200_000);
    expect(result).not.toBeNull();
    expect(result!.overCliff).toBe(false);
    expect(result!.ratio).toBeLessThan(0.33);
  });

  it("monthlyPaymentNTD is positive for valid inputs", () => {
    const result = computeAffordability(1000, 20, 2.5, 30, 100_000);
    expect(result!.monthlyPaymentNTD).toBeGreaterThan(0);
  });
});

// ── buildTiers ────────────────────────────────────────────────────────────────

describe("buildTiers", () => {
  it("returns exactly 3 tiers for pcts [5, 10, 20]", () => {
    const tiers = buildTiers(0, 30_000, 1000);
    expect(tiers).toHaveLength(3);
    expect(tiers.map((t) => t.pct)).toEqual([5, 10, 20]);
  });

  it("5% tier requires smaller down payment than 10% and 20%", () => {
    const tiers = buildTiers(0, 30_000, 1000);
    expect(tiers[0]!.requiredNTD).toBeLessThan(tiers[1]!.requiredNTD);
    expect(tiers[1]!.requiredNTD).toBeLessThan(tiers[2]!.requiredNTD);
  });

  it("months decrease monotonically with pct when savings < all thresholds", () => {
    // Savings are 0, so 5% is easiest (fewest months)
    const tiers = buildTiers(0, 30_000, 1000);
    expect(tiers[0]!.months).toBeLessThanOrEqual(tiers[1]!.months);
    expect(tiers[1]!.months).toBeLessThanOrEqual(tiers[2]!.months);
  });

  it("all tiers show months=0 when current savings exceed largest tier", () => {
    // 20% of 100萬 = NT$200,000; savings of 300,000 > all tiers
    const tiers = buildTiers(300_000, 30_000, 100);
    expect(tiers.every((t) => t.months === 0)).toBe(true);
  });

  it("all tiers show Infinity when monthly savings is 0 and no savings", () => {
    const tiers = buildTiers(0, 0, 1000);
    expect(tiers.every((t) => t.months === Infinity)).toBe(true);
  });

  it("edge case: price=0 → all requiredNTD=0, months=0", () => {
    const tiers = buildTiers(0, 30_000, 0);
    expect(tiers.every((t) => t.requiredNTD === 0 && t.months === 0)).toBe(true);
  });
});

// ── computeTimeline ───────────────────────────────────────────────────────────

describe("computeTimeline", () => {
  it("returns tiers, affordability, sensitivity", () => {
    const result = computeTimeline(500_000, 40_000, 1000, 100_000);
    expect(result.tiers).toHaveLength(3);
    expect(result.affordability).not.toBeNull();
    expect(result.sensitivity.tiers).toHaveLength(3);
  });

  it("affordability is null when income=0", () => {
    const result = computeTimeline(0, 30_000, 1000, 0);
    expect(result.affordability).toBeNull();
  });

  it("sensitivity tiers have equal or fewer months than base tiers (boosted savings)", () => {
    const result = computeTimeline(0, 30_000, 1000, 0);
    for (let i = 0; i < 3; i++) {
      const base = result.tiers[i]!.months;
      const boosted = result.sensitivity.tiers[i]!.months;
      if (base !== Infinity) {
        expect(boosted).toBeLessThanOrEqual(base);
      }
    }
  });

  it("sensitivity boost is NT$5,000", () => {
    const result = computeTimeline(0, 30_000, 1000, 0);
    expect(result.sensitivity.extraMonthlySavingsNTD).toBe(5_000);
  });

  it("edge case: price=0 → all tiers months=0", () => {
    const result = computeTimeline(0, 30_000, 0, 0);
    expect(result.tiers.every((t) => t.months === 0)).toBe(true);
  });

  it("edge case: current savings=0, monthly savings=0 → all Infinity", () => {
    const result = computeTimeline(0, 0, 1000, 0);
    expect(result.tiers.every((t) => t.months === Infinity)).toBe(true);
  });
});

// ── suggestTotalPriceWan ──────────────────────────────────────────────────────

describe("suggestTotalPriceWan", () => {
  it("50萬/坪 × 30坪 = 1500萬", () => {
    expect(suggestTotalPriceWan(50, 30)).toBe(1500);
  });

  it("uses default 30坪 when size omitted", () => {
    expect(suggestTotalPriceWan(40)).toBe(1200);
  });

  it("rounds to nearest integer 萬", () => {
    expect(suggestTotalPriceWan(33.333, 30)).toBe(1000);
  });

  it("returns 0 for zero or negative unit price", () => {
    expect(suggestTotalPriceWan(0)).toBe(0);
    expect(suggestTotalPriceWan(-10)).toBe(0);
  });

  it("returns 0 for zero or negative size", () => {
    expect(suggestTotalPriceWan(50, 0)).toBe(0);
    expect(suggestTotalPriceWan(50, -5)).toBe(0);
  });
});

// ── arrayMedian ──────────────────────────────────────────────────────────────

describe("arrayMedian", () => {
  it("returns null for empty array", () => {
    expect(arrayMedian([])).toBeNull();
  });

  it("returns single value for 1-element array", () => {
    expect(arrayMedian([42])).toBe(42);
  });

  it("returns middle value for odd-length array", () => {
    expect(arrayMedian([3, 1, 2])).toBe(2);
  });

  it("returns average of two middle values for even-length array", () => {
    expect(arrayMedian([1, 2, 3, 4])).toBe(2.5);
  });

  it("does not mutate input array", () => {
    const arr = [5, 3, 1, 4, 2];
    arrayMedian(arr);
    expect(arr).toEqual([5, 3, 1, 4, 2]);
  });
});

// ── formatMonths ──────────────────────────────────────────────────────────────

describe("formatMonths", () => {
  it("0 → already achieved", () => {
    expect(formatMonths(0)).toContain("已達標");
  });

  it("Infinity → cannot achieve", () => {
    expect(formatMonths(Infinity)).toContain("無法達成");
  });

  it("12 months → show months", () => {
    expect(formatMonths(12)).toContain("12");
    expect(formatMonths(12)).toContain("個月");
  });

  it("24 months → show months (boundary)", () => {
    expect(formatMonths(24)).toContain("個月");
  });

  it("36 months → show years approximation", () => {
    const result = formatMonths(36);
    expect(result).toContain("年");
    expect(result).toContain("3.0");
  });
});
