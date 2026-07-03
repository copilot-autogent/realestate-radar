import { describe, it, expect } from "vitest";
import {
  computeAffordability,
  tierLabel,
  formatGap,
  DOWN_PAYMENT_RATIO,
  INCOME_MULTIPLIER,
  AFFORDABLE_THRESHOLD,
  STRETCH_THRESHOLD,
} from "../lib/affordabilityUtils.js";

// Helper: 萬 → NT$
const wan = (n: number) => n * 10_000;

describe("computeAffordability", () => {
  // ── Basic tier classification ──────────────────────────────────────────────

  it("returns 'affordable' tier when ratio ≥ 90%", () => {
    // monthlyIncome 10萬, downPayment 300萬 → maxByIncome = 10萬*12*25 = 3000萬
    //                                           maxByDownPayment = 300萬/0.2 = 1500萬
    // maxAffordable = 1500萬; medianPrice = 1600萬 → ratio = 1500/1600 * 100 = 93.75%
    const result = computeAffordability(wan(1600), wan(10), wan(300));
    expect(result).not.toBeNull();
    expect(result!.tier).toBe("affordable");
    expect(result!.ratio).toBeGreaterThanOrEqual(AFFORDABLE_THRESHOLD);
  });

  it("returns 'stretch' tier when 60% ≤ ratio < 90%", () => {
    // maxByDownPayment = 300萬/0.2 = 1500萬; medianPrice = 2000萬
    // ratio = 1500/2000 * 100 = 75%
    const result = computeAffordability(wan(2000), wan(10), wan(300));
    expect(result).not.toBeNull();
    expect(result!.tier).toBe("stretch");
    expect(result!.ratio).toBeGreaterThanOrEqual(STRETCH_THRESHOLD);
    expect(result!.ratio).toBeLessThan(AFFORDABLE_THRESHOLD);
  });

  it("returns 'outOfRange' tier when ratio < 60%", () => {
    // maxByDownPayment = 100萬/0.2 = 500萬; medianPrice = 1000萬
    // ratio = 500/1000 * 100 = 50%
    const result = computeAffordability(wan(1000), wan(3), wan(100));
    expect(result).not.toBeNull();
    expect(result!.tier).toBe("outOfRange");
    expect(result!.ratio).toBeLessThan(STRETCH_THRESHOLD);
  });

  // ── Boundary conditions ────────────────────────────────────────────────────

  it("classifies as 'affordable' at exactly 90% ratio", () => {
    // maxAffordable = 900, medianPrice = 1000 → ratio = 90%
    const result = computeAffordability(wan(1000), wan(100), wan(180));
    // maxByIncome = 100*12*25 = 30000萬; maxByDownPayment = 180/0.2 = 900萬 → maxAffordable = 900萬
    // ratio = 900/1000 * 100 = 90%
    expect(result).not.toBeNull();
    expect(result!.ratio).toBeCloseTo(90, 1);
    expect(result!.tier).toBe("affordable");
  });

  it("classifies as 'stretch' just below 90% ratio", () => {
    // maxByDownPayment = 178.2/0.2 = 891萬; medianPrice = 1000萬 → ratio = 89.1%
    const result = computeAffordability(wan(1000), wan(100), wan(178.2));
    expect(result).not.toBeNull();
    expect(result!.ratio).toBeLessThan(AFFORDABLE_THRESHOLD);
    expect(result!.tier).toBe("stretch");
  });

  it("classifies as 'stretch' at exactly 60% ratio", () => {
    // maxByDownPayment = 120/0.2 = 600萬; medianPrice = 1000萬 → ratio = 60%
    const result = computeAffordability(wan(1000), wan(100), wan(120));
    expect(result).not.toBeNull();
    expect(result!.ratio).toBeCloseTo(60, 1);
    expect(result!.tier).toBe("stretch");
  });

  it("classifies as 'outOfRange' just below 60% ratio", () => {
    // maxByDownPayment = 119/0.2 = 595萬; medianPrice = 1000萬 → ratio = 59.5%
    const result = computeAffordability(wan(1000), wan(100), wan(119));
    expect(result).not.toBeNull();
    expect(result!.ratio).toBeLessThan(STRETCH_THRESHOLD);
    expect(result!.tier).toBe("outOfRange");
  });

  // ── DTI formula: min of two paths ─────────────────────────────────────────

  it("maxAffordable is limited by downPayment / 0.2 when that is smaller", () => {
    // downPayment = 100萬 → maxByDownPayment = 500萬
    // monthlyIncome = 20萬 → maxByIncome = 20*12*25 = 6000萬
    // min = 500萬
    const result = computeAffordability(wan(1000), wan(20), wan(100));
    expect(result).not.toBeNull();
    expect(result!.maxAffordable).toBe(wan(500));
  });

  it("maxAffordable is limited by monthlyIncome × 12 × 25 when that is smaller", () => {
    // monthlyIncome = 3萬 → maxByIncome = 3*12*25 = 900萬
    // downPayment = 300萬 → maxByDownPayment = 1500萬
    // min = 900萬
    const result = computeAffordability(wan(1500), wan(3), wan(300));
    expect(result).not.toBeNull();
    expect(result!.maxAffordable).toBe(wan(900));
  });

  it("when both limits are equal, maxAffordable equals either", () => {
    // monthlyIncome = 5萬 → maxByIncome = 5*12*25 = 1500萬
    // downPayment = 300萬 → maxByDownPayment = 1500萬
    const result = computeAffordability(wan(2000), wan(5), wan(300));
    expect(result).not.toBeNull();
    expect(result!.maxAffordable).toBe(wan(1500));
  });

  // ── Gap computation ────────────────────────────────────────────────────────

  it("gap is positive (shortfall) when district price exceeds maxAffordable", () => {
    // maxAffordable = 500萬, medianPrice = 1000萬 → gap = 500萬
    const result = computeAffordability(wan(1000), wan(2), wan(100));
    expect(result).not.toBeNull();
    expect(result!.gap).toBeGreaterThan(0);
    expect(result!.gap).toBeCloseTo(wan(500), -2);
  });

  it("gap is negative (surplus) when maxAffordable exceeds district price", () => {
    // maxByDownPayment = 500萬/0.2 = 2500萬; medianPrice = 1000萬
    // maxAffordable = min(2500萬, income*12*25); gap = 1000 - 2500 = -1500萬
    const result = computeAffordability(wan(1000), wan(20), wan(500));
    expect(result).not.toBeNull();
    expect(result!.gap).toBeLessThan(0);
  });

  it("gap is zero when maxAffordable exactly equals districtMedianPrice", () => {
    // maxByDownPayment = 200/0.2 = 1000萬; medianPrice = 1000萬
    // maxByIncome large enough not to limit
    const result = computeAffordability(wan(1000), wan(100), wan(200));
    expect(result).not.toBeNull();
    expect(result!.gap).toBe(0);
  });

  // ── NaN and invalid input guards ───────────────────────────────────────────

  it("returns null when districtMedianPrice is NaN", () => {
    expect(computeAffordability(NaN, wan(10), wan(300))).toBeNull();
  });

  it("returns null when monthlyIncome is NaN", () => {
    expect(computeAffordability(wan(1000), NaN, wan(300))).toBeNull();
  });

  it("returns null when downPayment is NaN", () => {
    expect(computeAffordability(wan(1000), wan(10), NaN)).toBeNull();
  });

  it("returns null when districtMedianPrice is zero", () => {
    expect(computeAffordability(0, wan(10), wan(300))).toBeNull();
  });

  it("returns null when monthlyIncome is zero", () => {
    expect(computeAffordability(wan(1000), 0, wan(300))).toBeNull();
  });

  it("returns null when downPayment is zero", () => {
    expect(computeAffordability(wan(1000), wan(10), 0)).toBeNull();
  });

  it("returns null when districtMedianPrice is negative", () => {
    expect(computeAffordability(-wan(1000), wan(10), wan(300))).toBeNull();
  });

  it("returns null when monthlyIncome is negative", () => {
    expect(computeAffordability(wan(1000), -wan(10), wan(300))).toBeNull();
  });

  it("returns null when downPayment is negative", () => {
    expect(computeAffordability(wan(1000), wan(10), -wan(300))).toBeNull();
  });

  it("returns null when districtMedianPrice is Infinity", () => {
    expect(computeAffordability(Infinity, wan(10), wan(300))).toBeNull();
  });

  // ── Constants consistency ──────────────────────────────────────────────────

  it("DOWN_PAYMENT_RATIO is 0.2", () => {
    expect(DOWN_PAYMENT_RATIO).toBe(0.2);
  });

  it("INCOME_MULTIPLIER is 25", () => {
    expect(INCOME_MULTIPLIER).toBe(25);
  });

  it("AFFORDABLE_THRESHOLD is 90", () => {
    expect(AFFORDABLE_THRESHOLD).toBe(90);
  });

  it("STRETCH_THRESHOLD is 60", () => {
    expect(STRETCH_THRESHOLD).toBe(60);
  });

  // ── Ratio range ────────────────────────────────────────────────────────────

  it("ratio can exceed 100% when buyer can afford more than the district price", () => {
    // maxAffordable = 2000萬, medianPrice = 1000萬 → ratio = 200%
    const result = computeAffordability(wan(1000), wan(100), wan(400));
    expect(result).not.toBeNull();
    expect(result!.ratio).toBeGreaterThan(100);
    expect(result!.tier).toBe("affordable");
  });
});

describe("tierLabel", () => {
  it("returns Chinese label for 'affordable'", () => {
    expect(tierLabel("affordable")).toContain("可負擔");
  });

  it("returns Chinese label for 'stretch'", () => {
    expect(tierLabel("stretch")).toContain("勉強可負擔");
  });

  it("returns Chinese label for 'outOfRange'", () => {
    expect(tierLabel("outOfRange")).toContain("超出能力範圍");
  });
});

describe("formatGap", () => {
  it("returns '尚差 X 萬' for positive gap (shortfall)", () => {
    const text = formatGap(wan(200));
    expect(text).toContain("尚差");
    expect(text).toContain("200");
  });

  it("returns '還有 X 萬空間' for negative gap (surplus)", () => {
    const text = formatGap(-wan(150));
    expect(text).toContain("還有");
    expect(text).toContain("150");
    expect(text).toContain("空間");
  });

  it("returns '剛好可負擔' for zero gap", () => {
    expect(formatGap(0)).toBe("剛好可負擔");
  });
});
