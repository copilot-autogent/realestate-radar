import { describe, it, expect } from "vitest";
import {
  computeNationalTimingSummary,
  formatYoY,
  subtractMonths,
} from "../lib/nationalTimingUtils.js";

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFeature(date: string, unitPrice = 300000): any {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [121.5, 25.0] },
    properties: { district: "大安區", city: "台北市", date, unitPrice },
  };
}

/**
 * Generate features spread across numMonths months starting from startYYYYMM,
 * with countPerMonth features each month.
 */
function makeMonthlyFeatures(
  startYYYYMM: string,
  numMonths: number,
  countPerMonth: number,
  unitPrice = 300000
): any[] {
  const features: any[] = [];
  let [y, mo] = startYYYYMM.split("-").map(Number) as [number, number];
  for (let m = 0; m < numMonths; m++) {
    const dateStr = `${y}-${String(mo).padStart(2, "0")}-15`;
    for (let c = 0; c < countPerMonth; c++) {
      features.push(makeFeature(dateStr, unitPrice));
    }
    mo++;
    if (mo > 12) { mo = 1; y++; }
  }
  return features;
}

// ── subtractMonths ────────────────────────────────────────────────────────────

describe("subtractMonths", () => {
  it("subtracts months within the same year", () => {
    expect(subtractMonths("2024-06", 3)).toBe("2024-03");
  });

  it("crosses a year boundary", () => {
    expect(subtractMonths("2024-03", 4)).toBe("2023-11");
  });

  it("subtracts 12 months (exact year)", () => {
    expect(subtractMonths("2024-06", 12)).toBe("2023-06");
  });

  it("subtracts 23 months", () => {
    expect(subtractMonths("2024-12", 23)).toBe("2023-01");
  });
});

// ── formatYoY ─────────────────────────────────────────────────────────────────

describe("formatYoY", () => {
  it("returns — for null", () => {
    expect(formatYoY(null)).toBe("—");
  });

  it("returns 持平 for near-zero change", () => {
    expect(formatYoY(0)).toBe("持平");
    expect(formatYoY(0.04)).toBe("持平");
    expect(formatYoY(-0.04)).toBe("持平");
  });

  it("formats negative change with ▼", () => {
    expect(formatYoY(-2.8)).toBe("▼2.8%");
  });

  it("formats positive change with ▲", () => {
    expect(formatYoY(5.2)).toBe("▲5.2%");
  });
});

// ── computeNationalTimingSummary ──────────────────────────────────────────────

describe("computeNationalTimingSummary", () => {
  it("returns sufficient=false for empty features", () => {
    const result = computeNationalTimingSummary([]);
    expect(result.sufficient).toBe(false);
    expect(result.yoyPriceChange).toBeNull();
    expect(result.buyerAdvantageRatio).toBeNull();
  });

  it("returns sufficient=false when < 24 months of data", () => {
    const features = makeMonthlyFeatures("2024-01", 12, 3);
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(false);
    expect(result.yoyPriceChange).toBeNull();
  });

  it("returns sufficient=true when ≥ 24 months of data", () => {
    const features = makeMonthlyFeatures("2023-01", 24, 3);
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(true);
  });

  it("computes yoyPriceChange correctly — falling prices give negative value", () => {
    // Trailing 12 months (2024-01 to 2024-12): unitPrice = 200000
    // Prior 12 months (2023-01 to 2023-12): unitPrice = 250000
    // YoY = (200000 - 250000) / 250000 * 100 = -20%
    const trailing = makeMonthlyFeatures("2024-01", 12, 3, 200000);
    const prior    = makeMonthlyFeatures("2023-01", 12, 3, 250000);
    const result = computeNationalTimingSummary([...trailing, ...prior]);
    expect(result.sufficient).toBe(true);
    expect(result.yoyPriceChange).not.toBeNull();
    expect(result.yoyPriceChange!).toBeCloseTo(-20, 0);
  });

  it("computes yoyPriceChange correctly — rising prices give positive value", () => {
    // Trailing 12 months: unitPrice = 330000
    // Prior 12 months: unitPrice = 300000
    // YoY = (330000 - 300000) / 300000 * 100 = 10%
    const trailing = makeMonthlyFeatures("2024-01", 12, 3, 330000);
    const prior    = makeMonthlyFeatures("2023-01", 12, 3, 300000);
    const result = computeNationalTimingSummary([...trailing, ...prior]);
    expect(result.sufficient).toBe(true);
    expect(result.yoyPriceChange).not.toBeNull();
    expect(result.yoyPriceChange!).toBeCloseTo(10, 0);
  });

  it("assigns green verdict when prices falling (price-driven)", () => {
    // Falling prices alone trigger green (priceFalling: yoy <= -1%)
    // Prior 12 months: high price + high volume (2023, peak=10)
    // Trailing 12 months: lower price + low volume (2024, count=2 → buyer advantaged)
    const features: any[] = [];
    for (let mo = 1; mo <= 12; mo++) {
      const date = `2023-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 10; c++) features.push(makeFeature(date, 300000));
    }
    for (let mo = 1; mo <= 12; mo++) {
      const date = `2024-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 2; c++) features.push(makeFeature(date, 280000));
    }
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(true);
    expect(result.verdict).toBe("green");
    expect(result.verdictLabel).toBe("買方有利");
    // YoY ≈ -6.67% — priceFalling triggers green regardless of buyerAdvantageRatio
    expect(result.yoyPriceChange!).toBeLessThan(-1);
  });

  it("assigns green verdict when prices flat but buyer-advantage months dominate", () => {
    // Same price across 24 months; low transaction volume in last 12 months creates
    // buyer-advantage months relative to higher-volume prior year
    const features: any[] = [];
    // Prior year (2023): high volume = 10/month (sets yearPeak for 2023)
    for (let mo = 1; mo <= 12; mo++) {
      const date = `2023-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 10; c++) features.push(makeFeature(date, 300000));
    }
    // Trailing year (2024): very low volume = 2/month < 70% of 2024 peak
    // To get all 12 months buyer-advantaged, we need a high 2024 peak in one month
    // and low volume in the rest. Set Jan 2024 to 10 (peak), rest to 2.
    features.push(...Array.from({length: 10}, () => makeFeature("2024-01-15", 300000)));
    for (let mo = 2; mo <= 12; mo++) {
      const date = `2024-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 2; c++) features.push(makeFeature(date, 300000));
    }
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(true);
    // YoY ≈ 0% (same price), but high buyer-advantage ratio → green
    expect(Math.abs(result.yoyPriceChange!)).toBeLessThan(1);
    expect(result.buyerAdvantageRatio!).toBeGreaterThan(50);
    expect(result.verdict).toBe("green");
  });

  it("assigns red verdict when prices rising and buyer-advantage ratio is low", () => {
    const features: any[] = [];
    // Prior 12 months: lower price, low volume
    for (let mo = 1; mo <= 12; mo++) {
      const date = `2023-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 3; c++) features.push(makeFeature(date, 250000));
    }
    // Trailing 12 months: higher price + very high volume (not buyer-advantaged)
    for (let mo = 1; mo <= 12; mo++) {
      const date = `2024-${String(mo).padStart(2, "0")}-15`;
      for (let c = 0; c < 10; c++) features.push(makeFeature(date, 300000));
    }
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(true);
    // YoY ≈ +20%, buyer advantage months low → red
    expect(result.verdict).toBe("red");
    expect(result.verdictLabel).toBe("賣方主導");
  });

  it("assigns yellow verdict for flat market", () => {
    // Same price for 24 months, medium volume (some buyer-advantaged but not majority)
    const features = makeMonthlyFeatures("2023-01", 24, 5, 300000);
    const result = computeNationalTimingSummary(features);
    expect(result.sufficient).toBe(true);
    expect(result.yoyPriceChange).not.toBeNull();
    expect(Math.abs(result.yoyPriceChange!)).toBeLessThan(1); // near zero
    expect(result.verdict).toBe("yellow");
  });

  it("handles full-width digit date strings (NFKC normalisation)", () => {
    // Mix of full-width digit dates (2024) with normal dates (2023)
    const priorFeatures = makeMonthlyFeatures("2023-01", 12, 3, 300000);
    const trailingFeatures = [
      // Full-width digits: ２０２４-０１-１５
      makeFeature("２０２４-０１-１５", 280000),
      ...makeMonthlyFeatures("2024-02", 11, 3, 280000),
    ];
    const result = computeNationalTimingSummary([...priorFeatures, ...trailingFeatures]);
    expect(result.sufficient).toBe(true);
    // Full-width date in 2024-01 should be parsed
    expect(result.yoyPriceChange).not.toBeNull();
  });

  it("gracefully handles features missing unitPrice or date", () => {
    const good = makeMonthlyFeatures("2023-01", 24, 3);
    const bad = [
      { type: "Feature", geometry: null, properties: { date: "2024-01-15" } },         // no unitPrice
      { type: "Feature", geometry: null, properties: { unitPrice: 300000 } },          // no date
      { type: "Feature", geometry: null, properties: null },                            // no properties
      null,                                                                              // null feature
    ];
    const result = computeNationalTimingSummary([...good, ...bad]);
    expect(result.sufficient).toBe(true);
  });

  it("buyerAdvantageRatio is 0–100", () => {
    const features = makeMonthlyFeatures("2022-01", 36, 5, 300000);
    const result = computeNationalTimingSummary(features);
    if (result.buyerAdvantageRatio !== null) {
      expect(result.buyerAdvantageRatio).toBeGreaterThanOrEqual(0);
      expect(result.buyerAdvantageRatio).toBeLessThanOrEqual(100);
    }
  });
});
