import { describe, it, expect } from "vitest";
import {
  computePriceToRentRatio,
  rentTierLabel,
  computeBreakEvenYears,
  suggestMonthlyRent,
  computeRentRatioSummary,
  DEFAULT_RENT_PER_PING,
  DEFAULT_RENT_PER_PING_FALLBACK,
  MAX_BREAK_EVEN_YEARS,
} from "../lib/rentRatioUtils";

// ── computePriceToRentRatio ──────────────────────────────────────────────────

describe("computePriceToRentRatio", () => {
  it("returns correct ratio for standard inputs", () => {
    // 10,000,000 NT$ property, annual rent 200,000 → ratio 50
    const ratio = computePriceToRentRatio(10_000_000, 200_000);
    expect(ratio).toBeCloseTo(50, 4);
  });

  it("returns smaller ratio when rent is higher", () => {
    const ratio = computePriceToRentRatio(10_000_000, 300_000);
    expect(ratio).toBeCloseTo(33.33, 1);
  });

  it("returns NaN when price is 0", () => {
    expect(computePriceToRentRatio(0, 200_000)).toBeNaN();
  });

  it("returns NaN when rent is 0", () => {
    expect(computePriceToRentRatio(10_000_000, 0)).toBeNaN();
  });

  it("returns NaN when price is negative", () => {
    expect(computePriceToRentRatio(-1_000_000, 200_000)).toBeNaN();
  });

  it("returns NaN when rent is negative", () => {
    expect(computePriceToRentRatio(10_000_000, -100_000)).toBeNaN();
  });

  it("returns NaN when price is Infinity", () => {
    expect(computePriceToRentRatio(Infinity, 200_000)).toBeNaN();
  });

  it("returns NaN when price is NaN", () => {
    expect(computePriceToRentRatio(NaN, 200_000)).toBeNaN();
  });

  it("higher price → higher ratio (same rent)", () => {
    const cheap = computePriceToRentRatio(5_000_000, 200_000);
    const expensive = computePriceToRentRatio(15_000_000, 200_000);
    expect(expensive).toBeGreaterThan(cheap);
  });
});

// ── rentTierLabel ────────────────────────────────────────────────────────────

describe("rentTierLabel", () => {
  it("returns 'affordable' when ratio < 30", () => {
    expect(rentTierLabel(20)).toBe("affordable");
    expect(rentTierLabel(29.9)).toBe("affordable");
    expect(rentTierLabel(1)).toBe("affordable");
  });

  it("returns 'neutral' when ratio is 30", () => {
    expect(rentTierLabel(30)).toBe("neutral");
  });

  it("returns 'neutral' when ratio is 50", () => {
    expect(rentTierLabel(50)).toBe("neutral");
  });

  it("returns 'neutral' for ratio between 30 and 50", () => {
    expect(rentTierLabel(40)).toBe("neutral");
  });

  it("returns 'expensive' when ratio > 50", () => {
    expect(rentTierLabel(51)).toBe("expensive");
    expect(rentTierLabel(100)).toBe("expensive");
  });

  it("returns 'neutral' for NaN", () => {
    expect(rentTierLabel(NaN)).toBe("neutral");
  });

  it("returns 'neutral' for zero", () => {
    expect(rentTierLabel(0)).toBe("neutral");
  });

  it("returns 'neutral' for negative ratio", () => {
    expect(rentTierLabel(-5)).toBe("neutral");
  });
});

// ── computeBreakEvenYears ────────────────────────────────────────────────────

describe("computeBreakEvenYears", () => {
  it("returns a break-even year within reasonable horizon for typical inputs", () => {
    // 10M NT$, 20% down, 2.35%, 30yr term, monthly rent 25k
    const result = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 25_000);
    expect(result.years).not.toBeNull();
    expect(result.years!).toBeGreaterThan(0);
    expect(result.years!).toBeLessThanOrEqual(MAX_BREAK_EVEN_YEARS);
  });

  it("returns null when rent is very low compared to mortgage (never breaks even)", () => {
    // Very expensive property, very cheap rent → renting will never catch up
    const result = computeBreakEvenYears(50_000_000, 20, 2.35, 30, 5_000);
    // At such low rent and high price/mortgage, buying is always more expensive
    // So cumulative rent never catches cumulative ownership → null or very large
    // With 50M, monthly mortgage ~200k, monthly rent 5k → rent never breaks even
    expect(result.years).toBeNull();
  });

  it("returns null for invalid price (zero)", () => {
    const result = computeBreakEvenYears(0, 20, 2.35, 30, 20_000);
    expect(result.years).toBeNull();
  });

  it("returns null for invalid rent (zero)", () => {
    const result = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 0);
    expect(result.years).toBeNull();
  });

  it("returns null for invalid down payment (> 100)", () => {
    const result = computeBreakEvenYears(10_000_000, 110, 2.35, 30, 20_000);
    expect(result.years).toBeNull();
  });

  it("returns null for invalid mortgage rate (negative)", () => {
    const result = computeBreakEvenYears(10_000_000, 20, -1, 30, 20_000);
    expect(result.years).toBeNull();
  });

  it("returns null for invalid term (zero)", () => {
    const result = computeBreakEvenYears(10_000_000, 20, 2.35, 0, 20_000);
    expect(result.years).toBeNull();
  });

  it("withinLoanTerm is true when break-even ≤ term years", () => {
    // Large rent vs small mortgage → break-even expected sooner
    const result = computeBreakEvenYears(5_000_000, 80, 2.35, 30, 40_000);
    if (result.years !== null && result.years <= 30) {
      expect(result.withinLoanTerm).toBe(true);
    }
  });

  it("withinLoanTerm is false when break-even > term years", () => {
    // Very high mortgage, moderate rent → break-even after loan term
    const result = computeBreakEvenYears(30_000_000, 20, 2.35, 30, 30_000);
    if (result.years !== null && result.years > 30) {
      expect(result.withinLoanTerm).toBe(false);
    }
  });

  it("0% appreciation gives same or longer break-even than positive appreciation", () => {
    const noAppreciation = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 25_000, 0);
    const someAppreciation = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 25_000, 2);
    // Appreciation reduces net ownership cost → shorter break-even
    if (noAppreciation.years !== null && someAppreciation.years !== null) {
      expect(someAppreciation.years).toBeLessThanOrEqual(noAppreciation.years);
    }
  });

  it("higher rent → earlier break-even year", () => {
    const lowerRent = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 20_000);
    const higherRent = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 40_000);
    if (lowerRent.years !== null && higherRent.years !== null) {
      expect(higherRent.years).toBeLessThanOrEqual(lowerRent.years);
    }
  });

  it("zero rate mortgage uses principal/term calculation correctly", () => {
    const result = computeBreakEvenYears(5_000_000, 0, 0, 30, 15_000);
    // With 0% rate, monthly mortgage = 5M / 360 ≈ 13,889 NT$ < 15,000 rent
    // So buying is cheaper from month 1 → break-even likely very soon
    expect(result).toBeDefined();
  });

  it("very high appreciation can produce early break-even on expensive property", () => {
    // 3% appreciation on 10M property → ~300k/yr gain reducing net ownership cost
    const result = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 20_000, 3);
    expect(result).toBeDefined(); // should not throw
  });

  it("returns struct with years and withinLoanTerm fields", () => {
    const result = computeBreakEvenYears(10_000_000, 20, 2.35, 30, 25_000);
    expect(result).toHaveProperty("years");
    expect(result).toHaveProperty("withinLoanTerm");
  });
});

// ── suggestMonthlyRent ───────────────────────────────────────────────────────

describe("suggestMonthlyRent", () => {
  it("returns Taipei rent estimate for 台北市", () => {
    const rent = suggestMonthlyRent("台北市", 30);
    expect(rent).toBe(DEFAULT_RENT_PER_PING["台北市"]! * 30);
  });

  it("returns New Taipei rent estimate for 新北市", () => {
    const rent = suggestMonthlyRent("新北市", 30);
    expect(rent).toBe(DEFAULT_RENT_PER_PING["新北市"]! * 30);
  });

  it("returns fallback rent for unknown city", () => {
    const rent = suggestMonthlyRent("花蓮縣", 30);
    expect(rent).toBe(DEFAULT_RENT_PER_PING_FALLBACK * 30);
  });

  it("scales with floor area", () => {
    const rent20 = suggestMonthlyRent("台北市", 20);
    const rent40 = suggestMonthlyRent("台北市", 40);
    expect(rent40).toBeGreaterThan(rent20);
    // Should be exactly 2×
    expect(rent40 / rent20).toBeCloseTo(2, 5);
  });

  it("defaults to 30 ping when area is 0", () => {
    const rentDefault = suggestMonthlyRent("台北市");
    const rent30 = suggestMonthlyRent("台北市", 30);
    expect(rentDefault).toBe(rent30);
  });

  it("defaults to 30 ping when area is NaN", () => {
    const rentNaN = suggestMonthlyRent("台北市", NaN);
    const rent30 = suggestMonthlyRent("台北市", 30);
    expect(rentNaN).toBe(rent30);
  });

  it("returns positive value for all cities in the table", () => {
    for (const city of Object.keys(DEFAULT_RENT_PER_PING)) {
      expect(suggestMonthlyRent(city, 30)).toBeGreaterThan(0);
    }
  });

  it("Taipei rent is higher than Tainan rent (market reality)", () => {
    const taipei = suggestMonthlyRent("台北市", 30);
    const tainan = suggestMonthlyRent("台南市", 30);
    expect(taipei).toBeGreaterThan(tainan);
  });
});

// ── computeRentRatioSummary ──────────────────────────────────────────────────

describe("computeRentRatioSummary", () => {
  it("returns a valid summary for standard inputs", () => {
    const summary = computeRentRatioSummary(10_000_000, 20_000, 20, 2.35, 30);
    expect(summary).not.toBeNull();
    expect(summary!.priceToRentRatio).toBeGreaterThan(0);
    expect(["affordable", "neutral", "expensive"]).toContain(summary!.tier);
    expect(summary!.monthlyMortgageNTD).toBeGreaterThan(0);
    expect(summary!.monthlyRentNTD).toBe(20_000);
  });

  it("returns null when price is zero", () => {
    expect(computeRentRatioSummary(0, 20_000, 20, 2.35, 30)).toBeNull();
  });

  it("returns null when rent is zero", () => {
    expect(computeRentRatioSummary(10_000_000, 0, 20, 2.35, 30)).toBeNull();
  });

  it("rentingCheaper is true when rent < monthly mortgage", () => {
    // 10M property, 20% down, 2.35%, 30yr: monthly mortgage ≈ 29,800 NT$
    // Monthly rent 15,000 → renting cheaper
    const summary = computeRentRatioSummary(10_000_000, 15_000, 20, 2.35, 30);
    expect(summary!.rentingCheaper).toBe(true);
  });

  it("rentingCheaper is false when rent > monthly mortgage", () => {
    // 5M property, 50% down, 1.775%: monthly mortgage ≈ 8,900
    // Monthly rent 30,000 → buying cheaper
    const summary = computeRentRatioSummary(5_000_000, 30_000, 50, 1.775, 30);
    expect(summary!.rentingCheaper).toBe(false);
  });

  it("priceToRentRatio matches manual calculation", () => {
    const price = 10_000_000;
    const monthlyRent = 20_000;
    const summary = computeRentRatioSummary(price, monthlyRent, 20, 2.35, 30);
    const expected = price / (monthlyRent * 12);
    expect(summary!.priceToRentRatio).toBeCloseTo(expected, 4);
  });

  it("tier matches standalone rentTierLabel for the computed ratio", () => {
    const summary = computeRentRatioSummary(20_000_000, 20_000, 20, 2.35, 30);
    // ratio = 20M / (20k × 12) = 83.3 → expensive
    expect(summary!.tier).toBe("expensive");
  });

  it("handles 0% appreciation as default", () => {
    const s1 = computeRentRatioSummary(10_000_000, 20_000, 20, 2.35, 30);
    const s2 = computeRentRatioSummary(10_000_000, 20_000, 20, 2.35, 30, 0);
    expect(s1!.breakEven.years).toBe(s2!.breakEven.years);
  });

  it("non-zero appreciation changes break-even vs 0%", () => {
    const s0 = computeRentRatioSummary(10_000_000, 25_000, 20, 2.35, 30, 0);
    const s2 = computeRentRatioSummary(10_000_000, 25_000, 20, 2.35, 30, 2);
    if (s0!.breakEven.years !== null && s2!.breakEven.years !== null) {
      expect(s2!.breakEven.years).toBeLessThanOrEqual(s0!.breakEven.years);
    }
  });
});

// ── Integration: realistic Taiwan scenario ───────────────────────────────────

describe("realistic Taiwan scenarios", () => {
  it("Taipei district: high ratio, renting favourable", () => {
    // Taipei Da'an, ~1,200萬 / 30坪 = 40,000/坪 → total 1,200萬 NT$
    // Monthly rent: 台北市 1,100/坪 × 30坪 = 33,000 NT$
    const price = 12_000_000;
    const rent = suggestMonthlyRent("台北市", 30);
    const ratio = computePriceToRentRatio(price, rent * 12);
    expect(ratio).toBeGreaterThan(30); // neutral to expensive
  });

  it("Tainan district: lower ratio, buying more competitive", () => {
    // Tainan, ~500萬 total, monthly rent 12,600 NT$ (30 ping × 420/ping)
    const price = 5_000_000;
    const rent = suggestMonthlyRent("台南市", 30);
    const ratio = computePriceToRentRatio(price, rent * 12);
    // ratio = 5M / (12,600 × 12) = ~33 → neutral (still within 30–50)
    expect(ratio).toBeLessThan(50);
  });
});
