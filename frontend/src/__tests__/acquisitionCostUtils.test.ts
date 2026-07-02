import { describe, it, expect } from "vitest";
import {
  renovationRatePerPing,
  computeAcquisitionCosts,
  computeAcquisitionCostSummary,
  computeTotalUpfrontNTD,
  formatNTD,
  formatPct,
  DEED_TAX_RATE,
  STAMP_DUTY_RATE,
  CONVEYANCING_FEE_NTD,
  AGENT_COMMISSION_RATE,
  MOVING_COST_NTD,
} from "../lib/acquisitionCostUtils.js";

// ── renovationRatePerPing ─────────────────────────────────────────────────────

describe("renovationRatePerPing", () => {
  it("returns 3000 for new building (0 years)", () => {
    expect(renovationRatePerPing(0)).toBe(3_000);
  });

  it("returns 3000 for 5-year-old building (boundary)", () => {
    expect(renovationRatePerPing(5)).toBe(3_000);
  });

  it("returns 5000 for 6-year-old building", () => {
    expect(renovationRatePerPing(6)).toBe(5_000);
  });

  it("returns 5000 for 20-year-old building (boundary)", () => {
    expect(renovationRatePerPing(20)).toBe(5_000);
  });

  it("returns 8000 for 21-year-old building", () => {
    expect(renovationRatePerPing(21)).toBe(8_000);
  });

  it("returns 8000 for 50-year-old building", () => {
    expect(renovationRatePerPing(50)).toBe(8_000);
  });

  it("returns 3000 for negative age (clamped to new)", () => {
    expect(renovationRatePerPing(-1)).toBe(3_000);
  });

  it("returns 3000 for NaN age (clamped to new)", () => {
    expect(renovationRatePerPing(NaN)).toBe(3_000);
  });
});

// ── computeAcquisitionCosts ───────────────────────────────────────────────────

describe("computeAcquisitionCosts", () => {
  it("returns 6 items", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    expect(items).toHaveLength(6);
  });

  it("deed-tax is 1.5% of price", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const deedTax = items.find((i) => i.key === "deed-tax")!;
    expect(deedTax.amountNTD).toBe(Math.round(10_000_000 * DEED_TAX_RATE));
  });

  it("stamp-duty is 0.1% of price", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const stampDuty = items.find((i) => i.key === "stamp-duty")!;
    expect(stampDuty.amountNTD).toBe(Math.round(10_000_000 * STAMP_DUTY_RATE));
  });

  it("conveyancing is fixed NT$20,000", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const conv = items.find((i) => i.key === "conveyancing")!;
    expect(conv.amountNTD).toBe(CONVEYANCING_FEE_NTD);
  });

  it("agent-commission is 1% of price when included", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10, true);
    const agent = items.find((i) => i.key === "agent-commission")!;
    expect(agent.amountNTD).toBe(Math.round(10_000_000 * AGENT_COMMISSION_RATE));
  });

  it("agent-commission is 0 when excluded", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10, false);
    const agent = items.find((i) => i.key === "agent-commission")!;
    expect(agent.amountNTD).toBe(0);
  });

  it("agent-commission is marked as optional", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const agent = items.find((i) => i.key === "agent-commission")!;
    expect(agent.isOptional).toBe(true);
  });

  it("renovation uses 5000/坪 for 10-year building", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const reno = items.find((i) => i.key === "renovation")!;
    expect(reno.amountNTD).toBe(30 * 5_000);
  });

  it("renovation uses 8000/坪 for 30-year building", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 30);
    const reno = items.find((i) => i.key === "renovation")!;
    expect(reno.amountNTD).toBe(30 * 8_000);
  });

  it("renovation uses 3000/坪 for new building", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 3);
    const reno = items.find((i) => i.key === "renovation")!;
    expect(reno.amountNTD).toBe(30 * 3_000);
  });

  it("moving cost is fixed NT$25,000", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const moving = items.find((i) => i.key === "moving")!;
    expect(moving.amountNTD).toBe(MOVING_COST_NTD);
  });

  it("price=0 yields zero for percentage-based items", () => {
    const items = computeAcquisitionCosts(0, 30, 10);
    const deedTax = items.find((i) => i.key === "deed-tax")!;
    expect(deedTax.amountNTD).toBe(0);
    const stampDuty = items.find((i) => i.key === "stamp-duty")!;
    expect(stampDuty.amountNTD).toBe(0);
  });

  it("ping=0 yields zero renovation", () => {
    const items = computeAcquisitionCosts(10_000_000, 0, 10);
    const reno = items.find((i) => i.key === "renovation")!;
    expect(reno.amountNTD).toBe(0);
  });

  it("very small 坪數 (1坪) still computes correctly", () => {
    const items = computeAcquisitionCosts(5_000_000, 1, 5);
    const reno = items.find((i) => i.key === "renovation")!;
    expect(reno.amountNTD).toBe(1 * 3_000);
  });

  it("fixed fees are unchanged regardless of price", () => {
    const itemsLow  = computeAcquisitionCosts(1_000_000,   30, 10);
    const itemsHigh = computeAcquisitionCosts(100_000_000, 30, 10);
    expect(itemsLow .find((i) => i.key === "conveyancing")!.amountNTD).toBe(CONVEYANCING_FEE_NTD);
    expect(itemsHigh.find((i) => i.key === "conveyancing")!.amountNTD).toBe(CONVEYANCING_FEE_NTD);
    expect(itemsLow .find((i) => i.key === "moving")!.amountNTD).toBe(MOVING_COST_NTD);
    expect(itemsHigh.find((i) => i.key === "moving")!.amountNTD).toBe(MOVING_COST_NTD);
  });

  it("all non-optional items have isOptional=false", () => {
    const items = computeAcquisitionCosts(10_000_000, 30, 10);
    const mandatory = items.filter((i) => i.key !== "agent-commission");
    expect(mandatory.every((i) => i.isOptional === false)).toBe(true);
  });

  it("NaN price yields zero for percentage-based items but fixed fees remain", () => {
    const items = computeAcquisitionCosts(NaN, 30, 10);
    const deedTax = items.find((i) => i.key === "deed-tax")!;
    expect(deedTax.amountNTD).toBe(0);
    const conveyancing = items.find((i) => i.key === "conveyancing")!;
    expect(conveyancing.amountNTD).toBe(CONVEYANCING_FEE_NTD);
  });
});

// ── computeAcquisitionCostSummary ─────────────────────────────────────────────

describe("computeAcquisitionCostSummary", () => {
  it("totalHiddenCostsNTD equals sum of item amounts", () => {
    const result = computeAcquisitionCostSummary(10_000_000, 30, 10);
    const manual = result.items.reduce((s, i) => s + i.amountNTD, 0);
    expect(result.totalHiddenCostsNTD).toBe(manual);
  });

  it("hiddenCostsPct = total / price", () => {
    const price = 10_000_000;
    const result = computeAcquisitionCostSummary(price, 30, 10);
    expect(result.hiddenCostsPct).toBeCloseTo(result.totalHiddenCostsNTD / price, 6);
  });

  it("hiddenCostsPct is 0 when price=0", () => {
    const result = computeAcquisitionCostSummary(0, 30, 10);
    expect(result.hiddenCostsPct).toBe(0);
  });

  it("total is higher for old building than new (larger renovation)", () => {
    const newBldg = computeAcquisitionCostSummary(10_000_000, 30, 2);
    const oldBldg = computeAcquisitionCostSummary(10_000_000, 30, 30);
    expect(oldBldg.totalHiddenCostsNTD).toBeGreaterThan(newBldg.totalHiddenCostsNTD);
  });

  it("excluding agent commission lowers total", () => {
    const withAgent    = computeAcquisitionCostSummary(10_000_000, 30, 10, true);
    const withoutAgent = computeAcquisitionCostSummary(10_000_000, 30, 10, false);
    expect(withAgent.totalHiddenCostsNTD).toBeGreaterThan(withoutAgent.totalHiddenCostsNTD);
  });

  it("typical 1000萬 / 30坪 / 10年 hidden costs are 3–6% of price", () => {
    const result = computeAcquisitionCostSummary(10_000_000, 30, 10);
    expect(result.hiddenCostsPct).toBeGreaterThan(0.03);
    expect(result.hiddenCostsPct).toBeLessThan(0.06);
  });
});

// ── computeTotalUpfrontNTD ────────────────────────────────────────────────────

describe("computeTotalUpfrontNTD", () => {
  it("20% down + hidden costs sums correctly", () => {
    const price = 10_000_000;
    const hidden = 500_000;
    expect(computeTotalUpfrontNTD(price, 20, hidden)).toBe(2_000_000 + 500_000);
  });

  it("returns just hidden costs for 0% down payment", () => {
    expect(computeTotalUpfrontNTD(10_000_000, 0, 200_000)).toBe(200_000);
  });

  it("returns 0 when price=0 and hidden=0", () => {
    expect(computeTotalUpfrontNTD(0, 20, 0)).toBe(0);
  });

  it("handles NaN price gracefully (returns hidden costs only)", () => {
    expect(computeTotalUpfrontNTD(NaN, 20, 100_000)).toBe(100_000);
  });
});

// ── formatNTD ─────────────────────────────────────────────────────────────────

describe("formatNTD", () => {
  it("formats amounts ≥ 10000 as 萬 with 1dp", () => {
    expect(formatNTD(150_000)).toBe("NT$15.0萬");
  });

  it("formats 1500000 correctly", () => {
    expect(formatNTD(1_500_000)).toBe("NT$150.0萬");
  });

  it("formats small amounts as plain NT$", () => {
    expect(formatNTD(8_000)).toBe("NT$8,000");
  });

  it("returns NT$0 for negative", () => {
    expect(formatNTD(-1)).toBe("NT$0");
  });

  it("returns NT$0 for NaN", () => {
    expect(formatNTD(NaN)).toBe("NT$0");
  });

  it("formats exactly 10000 as 1.0萬", () => {
    expect(formatNTD(10_000)).toBe("NT$1.0萬");
  });
});

// ── formatPct ─────────────────────────────────────────────────────────────────

describe("formatPct", () => {
  it("formats fraction as percentage with 1dp", () => {
    expect(formatPct(0.035)).toBe("3.5%");
  });

  it("formats 0 as 0.0%", () => {
    expect(formatPct(0)).toBe("0.0%");
  });

  it("returns '—%' for NaN", () => {
    expect(formatPct(NaN)).toBe("—%");
  });

  it("formats 1.0 as 100.0%", () => {
    expect(formatPct(1.0)).toBe("100.0%");
  });
});
