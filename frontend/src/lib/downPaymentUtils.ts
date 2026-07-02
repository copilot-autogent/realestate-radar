/**
 * Pure utility functions for the down-payment timeline calculator (issue #111).
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * Scenario: first-time buyer asks "how long until I can afford a down payment
 * in my target district, given my current savings rate?"
 *
 * Inputs:
 *   currentSavingsNTD  — savings already accumulated (NT$)
 *   monthlySavingsNTD  — amount saved each month (NT$)
 *   totalPriceWan      — target property total price (萬 NT$, 1 萬 = NT$10,000)
 *   monthlyIncomeNTD   — monthly gross income for affordability check (NT$, 0 = skip)
 *   loanRatePercent    — annual mortgage interest rate (e.g. 2.5 for 2.5%)
 *   loanYears          — mortgage term in years (default 30)
 */

// ── Types ────────────────────────────────────────────────────────────────────

/** The three standard down-payment tiers used in Taiwan. */
export type DownPaymentPct = 5 | 10 | 20;

export interface TimelineTier {
  pct: DownPaymentPct;
  /** Required down payment in NT$ */
  requiredNTD: number;
  /** Months until target reached; 0 = already have enough; Infinity = not possible (monthlySavings ≤ 0) */
  months: number;
  /** Color code based on months threshold */
  color: "green" | "amber" | "red";
}

export interface AffordabilityCheck {
  /** Monthly mortgage payment (NT$) */
  monthlyPaymentNTD: number;
  /** Monthly payment as fraction of monthly income (e.g. 0.33 = 33%) */
  ratio: number;
  /** Whether monthly payment exceeds 33% of income */
  overCliff: boolean;
}

export interface SensitivityResult {
  /** Extra monthly savings added (NT$) */
  extraMonthlySavingsNTD: number;
  /** Tiers recalculated with boosted savings rate */
  tiers: TimelineTier[];
}

export interface TimelineResult {
  tiers: TimelineTier[];
  /** Affordability check; null when monthlyIncomeNTD is 0 */
  affordability: AffordabilityCheck | null;
  /** Sensitivity: effect of adding NT$5,000/month to savings rate */
  sensitivity: SensitivityResult;
}

// ── Down payment amount ──────────────────────────────────────────────────────

/**
 * Compute down payment required in NT$.
 * @param totalPriceWan  Total property price in 萬 NT$
 * @param pct            Down-payment percentage (5, 10, or 20)
 */
export function computeDownPaymentNTD(totalPriceWan: number, pct: DownPaymentPct): number {
  if (!Number.isFinite(totalPriceWan) || totalPriceWan <= 0) return 0;
  return totalPriceWan * 10_000 * (pct / 100);
}

// ── Months to goal ───────────────────────────────────────────────────────────

/**
 * Compute months until savings reaches `targetNTD`.
 * Returns 0 when currentSavingsNTD ≥ targetNTD (already there).
 * Returns Infinity when monthlySavingsNTD ≤ 0 (no progress possible).
 */
export function computeMonthsToGoal(
  currentSavingsNTD: number,
  monthlySavingsNTD: number,
  targetNTD: number
): number {
  if (!Number.isFinite(targetNTD) || targetNTD <= 0) return 0;
  if (!Number.isFinite(currentSavingsNTD)) return Infinity;
  if (currentSavingsNTD >= targetNTD) return 0;
  if (!Number.isFinite(monthlySavingsNTD) || monthlySavingsNTD <= 0) return Infinity;
  const gap = targetNTD - currentSavingsNTD;
  return Math.ceil(gap / monthlySavingsNTD);
}

// ── Tier color coding ────────────────────────────────────────────────────────

/**
 * Map months-to-goal to a color band.
 * ≤ 24 months → green; 24 < x ≤ 48 → amber; > 48 or Infinity → red.
 */
export function monthsToColor(months: number): "green" | "amber" | "red" {
  if (months === Infinity || months > 48) return "red";
  if (months > 24) return "amber";
  return "green";
}

// ── Monthly mortgage payment ─────────────────────────────────────────────────

/**
 * Standard amortization monthly payment formula.
 * @param loanWan        Loan principal in 萬 NT$
 * @param ratePercent    Annual interest rate (e.g. 2.5 for 2.5%)
 * @param years          Loan term in years
 * @returns Monthly payment in NT$
 */
export function computeMonthlyMortgagePayment(
  loanWan: number,
  ratePercent: number,
  years: number
): number {
  if (!Number.isFinite(loanWan) || loanWan <= 0) return 0;
  if (!Number.isFinite(years) || years <= 0) return 0;
  if (!Number.isFinite(ratePercent) || ratePercent <= 0) {
    // Zero-rate edge case: simple principal division
    return loanWan * 10_000 / (years * 12);
  }
  const principal = loanWan * 10_000;
  const monthlyRate = ratePercent / 100 / 12;
  const n = years * 12;
  return principal * (monthlyRate * Math.pow(1 + monthlyRate, n)) /
    (Math.pow(1 + monthlyRate, n) - 1);
}

// ── Affordability check ──────────────────────────────────────────────────────

const AFFORDABILITY_CLIFF = 0.33; // 33% debt-to-income threshold

/**
 * Compute mortgage-to-income ratio for a given down-payment tier.
 * @param totalPriceWan     Total price in 萬
 * @param dpPct             Down-payment percentage used (affects loan size)
 * @param ratePercent       Annual rate %
 * @param years             Loan years
 * @param monthlyIncomeNTD  Monthly gross income in NT$; 0 means skip check
 */
export function computeAffordability(
  totalPriceWan: number,
  dpPct: DownPaymentPct,
  ratePercent: number,
  years: number,
  monthlyIncomeNTD: number
): AffordabilityCheck | null {
  if (monthlyIncomeNTD <= 0) return null;
  if (!Number.isFinite(totalPriceWan) || totalPriceWan <= 0) return null;

  const loanWan = totalPriceWan * (1 - dpPct / 100);
  const monthlyPaymentNTD = computeMonthlyMortgagePayment(loanWan, ratePercent, years);
  const ratio = monthlyPaymentNTD / monthlyIncomeNTD;

  return {
    monthlyPaymentNTD,
    ratio,
    overCliff: ratio > AFFORDABILITY_CLIFF,
  };
}

// ── Build timeline tiers ─────────────────────────────────────────────────────

const DOWN_PAYMENT_PCTS: DownPaymentPct[] = [5, 10, 20];

/**
 * Build timeline tiers for the three standard down-payment percentages.
 */
export function buildTiers(
  currentSavingsNTD: number,
  monthlySavingsNTD: number,
  totalPriceWan: number
): TimelineTier[] {
  return DOWN_PAYMENT_PCTS.map((pct) => {
    const requiredNTD = computeDownPaymentNTD(totalPriceWan, pct);
    const months = computeMonthsToGoal(currentSavingsNTD, monthlySavingsNTD, requiredNTD);
    return {
      pct,
      requiredNTD,
      months,
      color: monthsToColor(months),
    };
  });
}

// ── Main timeline calculator ─────────────────────────────────────────────────

/**
 * Compute full down-payment timeline result.
 *
 * @param currentSavingsNTD  Current savings in NT$
 * @param monthlySavingsNTD  Monthly savings in NT$
 * @param totalPriceWan      Target property total price in 萬
 * @param monthlyIncomeNTD   Monthly gross income in NT$ (0 = skip affordability)
 * @param loanRatePercent    Annual mortgage rate % for affordability check (default 2.5)
 * @param loanYears          Mortgage term years for affordability check (default 30)
 */
export function computeTimeline(
  currentSavingsNTD: number,
  monthlySavingsNTD: number,
  totalPriceWan: number,
  monthlyIncomeNTD: number = 0,
  loanRatePercent: number = 2.5,
  loanYears: number = 30
): TimelineResult {
  const tiers = buildTiers(currentSavingsNTD, monthlySavingsNTD, totalPriceWan);

  // Affordability check uses the 20% down-payment tier (standard conventional loan)
  const affordability = computeAffordability(
    totalPriceWan,
    20,
    loanRatePercent,
    loanYears,
    monthlyIncomeNTD
  );

  // Sensitivity: +NT$5,000/month
  const SENSITIVITY_BOOST = 5_000;
  const boostedTiers = buildTiers(
    currentSavingsNTD,
    monthlySavingsNTD + SENSITIVITY_BOOST,
    totalPriceWan
  );
  const sensitivity: SensitivityResult = {
    extraMonthlySavingsNTD: SENSITIVITY_BOOST,
    tiers: boostedTiers,
  };

  return { tiers, affordability, sensitivity };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Derive a suggested total price in 萬 from district median unit price and apartment size.
 * Used to auto-fill the total price input when a district is selected on the map.
 *
 * @param medianUnitPriceWan  Median unit price in 萬/坪
 * @param sizePing            Apartment size in 坪 (default 30)
 */
export function suggestTotalPriceWan(medianUnitPriceWan: number, sizePing: number = 30): number {
  if (!Number.isFinite(medianUnitPriceWan) || medianUnitPriceWan <= 0) return 0;
  if (!Number.isFinite(sizePing) || sizePing <= 0) return 0;
  return Math.round(medianUnitPriceWan * sizePing);
}

/**
 * Compute median from a non-empty sorted or unsorted array of numbers.
 * Returns null for empty arrays.
 */
export function arrayMedian(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

/** Format a number of months into a human-readable label (e.g. "18 個月", "約 3.5 年"). */
export function formatMonths(months: number): string {
  if (months === 0) return "已達標 ✓";
  if (months === Infinity) return "無法達成";
  if (months <= 24) return `${months} 個月`;
  const years = months / 12;
  return `約 ${years.toFixed(1)} 年`;
}
