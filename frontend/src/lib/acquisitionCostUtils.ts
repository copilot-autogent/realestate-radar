/**
 * Pure utility functions for the hidden acquisition cost calculator (issue #121).
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * Scenario: first-time buyer (首購族) asks "what are all the one-time fees
 * I need to budget on top of the down payment?"
 *
 * Inputs:
 *   priceNTD            — total purchase price in NT$ (e.g. 10_000_000 for 1,000萬)
 *   pingSqm             — apartment size in 坪
 *   buildingAgeYears    — building age in years (used for renovation estimate)
 *   includeAgentCommission — whether to include 仲介費 (default true)
 */

// ── Types ────────────────────────────────────────────────────────────────────

export interface AcquisitionCost {
  /** Machine-readable identifier */
  key: string;
  /** Chinese cost-item label */
  label: string;
  /** Computed amount in NT$ */
  amountNTD: number;
  /** Human-readable description of how the amount is derived */
  rateDescription: string;
  /** Whether this item can be toggled off (e.g. 仲介費 when buying direct) */
  isOptional: boolean;
}

export interface AcquisitionCostResult {
  items: AcquisitionCost[];
  /** Sum of all included item amounts */
  totalHiddenCostsNTD: number;
  /** totalHiddenCostsNTD / priceNTD; 0 when price is 0 */
  hiddenCostsPct: number;
}

// ── Rate constants ───────────────────────────────────────────────────────────

/** 契稅 approximation: 1.5% of transaction price (6% × ~25% building-value ratio) */
export const DEED_TAX_RATE = 0.015;

/** 印花稅: flat 0.1% of transaction price */
export const STAMP_DUTY_RATE = 0.001;

/** 代書費: fixed NT$20,000 (midpoint of NT$15,000–25,000 market range) */
export const CONVEYANCING_FEE_NTD = 20_000;

/** 仲介費: 1% of price (lower bound; buyer's side only) */
export const AGENT_COMMISSION_RATE = 0.01;

/** 搬家費: fixed NT$25,000 (midpoint of NT$20,000–40,000 estimate) */
export const MOVING_COST_NTD = 25_000;

// ── Renovation rate by building age ─────────────────────────────────────────

/**
 * Returns the renovation cost per 坪 (NT$) based on building age.
 *
 * | Building age | Rate (NT$/坪) | Rationale |
 * |---|---|---|
 * | 0–5 years   | 3,000 | New/near-new; cosmetic touch-up only |
 * | 6–20 years  | 5,000 | Mid-age; kitchen/bath refresh |
 * | 21+ years   | 8,000 | Old building; structural + plumbing overhaul |
 */
export function renovationRatePerPing(buildingAgeYears: number): number {
  if (!Number.isFinite(buildingAgeYears) || buildingAgeYears < 0) return 3_000;
  if (buildingAgeYears <= 5) return 3_000;
  if (buildingAgeYears <= 20) return 5_000;
  return 8_000;
}

// ── Core computation ─────────────────────────────────────────────────────────

/**
 * Compute itemized acquisition costs.
 *
 * @param priceNTD               Total purchase price in NT$
 * @param pingSqm                Apartment size in 坪
 * @param buildingAgeYears       Building age in years
 * @param includeAgentCommission Whether to include 仲介費 (default true)
 * @returns Array of cost items; amounts are 0 when inputs are invalid.
 */
export function computeAcquisitionCosts(
  priceNTD: number,
  pingSqm: number,
  buildingAgeYears: number,
  includeAgentCommission: boolean = true
): AcquisitionCost[] {
  const safePrice = Number.isFinite(priceNTD) && priceNTD > 0 ? priceNTD : 0;
  const safePing  = Number.isFinite(pingSqm)  && pingSqm  > 0 ? pingSqm  : 0;
  const safeAge   = Number.isFinite(buildingAgeYears) && buildingAgeYears >= 0
    ? buildingAgeYears
    : 0;

  const deedTax         = Math.round(safePrice * DEED_TAX_RATE);
  const stampDuty       = Math.round(safePrice * STAMP_DUTY_RATE);
  const agentCommission = Math.round(safePrice * AGENT_COMMISSION_RATE);
  const renovRate       = renovationRatePerPing(safeAge);
  const renovation      = Math.round(safePing * renovRate);

  return [
    {
      key: "deed-tax",
      label: "契稅",
      amountNTD: deedTax,
      rateDescription: `${(DEED_TAX_RATE * 100).toFixed(1)}% × 成交總價（建物現值 6% 換算）`,
      isOptional: false,
    },
    {
      key: "stamp-duty",
      label: "印花稅",
      amountNTD: stampDuty,
      rateDescription: `${(STAMP_DUTY_RATE * 100).toFixed(1)}% × 成交總價`,
      isOptional: false,
    },
    {
      key: "conveyancing",
      label: "代書費",
      amountNTD: CONVEYANCING_FEE_NTD,
      rateDescription: `固定估算（市場行情 NT$15,000–25,000）`,
      isOptional: false,
    },
    {
      key: "agent-commission",
      label: "仲介費",
      amountNTD: includeAgentCommission ? agentCommission : 0,
      rateDescription: `${(AGENT_COMMISSION_RATE * 100).toFixed(0)}% × 成交總價（如透過仲介）`,
      isOptional: true,
    },
    {
      key: "renovation",
      label: "裝修預算",
      amountNTD: renovation,
      rateDescription: `NT$${renovRate.toLocaleString()}/坪 × ${safePing}坪（屋齡${safeAge}年）`,
      isOptional: false,
    },
    {
      key: "moving",
      label: "搬家費",
      amountNTD: MOVING_COST_NTD,
      rateDescription: `固定估算（市場行情 NT$20,000–40,000）`,
      isOptional: false,
    },
  ];
}

// ── Summary result ───────────────────────────────────────────────────────────

/**
 * Compute acquisition costs and produce a summary result.
 *
 * @param priceNTD               Total purchase price in NT$
 * @param pingSqm                Apartment size in 坪
 * @param buildingAgeYears       Building age in years
 * @param includeAgentCommission Whether to include 仲介費 (default true)
 */
export function computeAcquisitionCostSummary(
  priceNTD: number,
  pingSqm: number,
  buildingAgeYears: number,
  includeAgentCommission: boolean = true
): AcquisitionCostResult {
  const items = computeAcquisitionCosts(priceNTD, pingSqm, buildingAgeYears, includeAgentCommission);
  const totalHiddenCostsNTD = items.reduce((sum, item) => sum + item.amountNTD, 0);
  const safePrice = Number.isFinite(priceNTD) && priceNTD > 0 ? priceNTD : 0;
  const hiddenCostsPct = safePrice > 0 ? totalHiddenCostsNTD / safePrice : 0;
  return { items, totalHiddenCostsNTD, hiddenCostsPct };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Format NT$ amount as a compact human-readable label.
 * Amounts ≥ 10,000 are shown in 萬 with one decimal place.
 *
 * @example
 * formatNTD(20_000)      → "NT$2.0萬"
 * formatNTD(1_500_000)   → "NT$150.0萬"
 * formatNTD(8_000)       → "NT$8,000"
 */
export function formatNTD(amountNTD: number): string {
  if (!Number.isFinite(amountNTD) || amountNTD < 0) return "NT$0";
  if (amountNTD >= 10_000) {
    return `NT$${(amountNTD / 10_000).toFixed(1)}萬`;
  }
  return `NT$${amountNTD.toLocaleString()}`;
}

/**
 * Format a fraction as a percentage string with one decimal place.
 *
 * @example
 * formatPct(0.0345) → "3.5%"
 */
export function formatPct(fraction: number): string {
  if (!Number.isFinite(fraction)) return "—%";
  return `${(fraction * 100).toFixed(1)}%`;
}

/**
 * Compute total upfront cash needed = down payment (at given %) + hidden costs.
 *
 * @param priceNTD             Total purchase price in NT$
 * @param downPaymentPct       Down-payment percentage (0–100)
 * @param hiddenCostsNTD       Pre-computed total hidden costs in NT$
 */
export function computeTotalUpfrontNTD(
  priceNTD: number,
  downPaymentPct: number,
  hiddenCostsNTD: number
): number {
  const safePrice = Number.isFinite(priceNTD)       && priceNTD       > 0 ? priceNTD       : 0;
  const safePct   = Number.isFinite(downPaymentPct) && downPaymentPct >= 0 ? downPaymentPct : 0;
  const safeCosts = Number.isFinite(hiddenCostsNTD) && hiddenCostsNTD >= 0 ? hiddenCostsNTD : 0;
  const dp = Math.round(safePrice * (safePct / 100));
  return dp + safeCosts;
}
