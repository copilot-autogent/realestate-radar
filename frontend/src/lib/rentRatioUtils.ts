/**
 * Pure utility functions for the 租售比試算器 (Price-to-Rent Ratio Calculator) — issue #143.
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * Key concepts:
 *  - Price-to-rent ratio (租售比): medianPrice / annualRent
 *    Taiwan benchmarks: <30 = buying favourable, 30-50 = neutral, >50 = renting favourable
 *  - Break-even horizon: year when cumulative cost-of-ownership equals cumulative rent
 *  - Reuses computeMonthlyPayment from loanProgramUtils (no duplication)
 */

import { computeMonthlyPayment } from "./loanProgramUtils.js";

// ── Types ────────────────────────────────────────────────────────────────────

export type RentTierLabel = "affordable" | "neutral" | "expensive";

export interface BreakEvenResult {
  /** Year at which cumulative ownership cost equals cumulative rent cost; null = never */
  years: number | null;
  /** Whether break-even is within the loan term (30 years) */
  withinLoanTerm: boolean;
}

export interface RentRatioSummary {
  priceToRentRatio: number;
  tier: RentTierLabel;
  breakEven: BreakEvenResult;
  monthlyMortgageNTD: number;
  monthlyRentNTD: number;
  /** true = renting is cheaper month-to-month */
  rentingCheaper: boolean;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** Maximum horizon checked for break-even (years). Beyond this → null (never). */
export const MAX_BREAK_EVEN_YEARS = 60;

/**
 * Rough per-ping monthly rent estimates by city (NT$/坪).
 * Static lookup for first-time buyers who haven't specified a rent.
 * Based on approximate Taiwan market data (2024 reference).
 */
export const DEFAULT_RENT_PER_PING: Record<string, number> = {
  台北市: 1100,
  新北市: 750,
  桃園市: 550,
  台中市: 580,
  台南市: 420,
  高雄市: 440,
};

/** Fallback per-ping rent when city is not in the table */
export const DEFAULT_RENT_PER_PING_FALLBACK = 600;

// ── Core computations ────────────────────────────────────────────────────────

/**
 * Compute the price-to-rent ratio (租售比).
 * Formula: totalPriceNTD / annualRentNTD
 *
 * @param totalPriceNTD  Total property price in NT$
 * @param annualRentNTD  Annual rent in NT$ (= monthly rent × 12)
 * @returns Ratio; NaN when inputs are invalid/zero
 */
export function computePriceToRentRatio(
  totalPriceNTD: number,
  annualRentNTD: number,
): number {
  if (
    !Number.isFinite(totalPriceNTD) || totalPriceNTD <= 0 ||
    !Number.isFinite(annualRentNTD) || annualRentNTD <= 0
  ) return NaN;
  return totalPriceNTD / annualRentNTD;
}

/**
 * Classify the price-to-rent ratio into a tier label.
 *
 * Taiwan benchmarks:
 *   < 30   → "affordable" (buying makes more financial sense)
 *   30–50  → "neutral"
 *   > 50   → "expensive" (renting makes more financial sense)
 *
 * @param ratio  Price-to-rent ratio (must be > 0 and finite)
 */
export function rentTierLabel(ratio: number): RentTierLabel {
  if (!Number.isFinite(ratio) || ratio <= 0) return "neutral";
  if (ratio < 30) return "affordable";
  if (ratio <= 50) return "neutral";
  return "expensive";
}

/**
 * Compute the break-even horizon (損益平衡年限).
 *
 * Simplified model iterating year-by-year:
 *   cumulative ownership cost = Σ (monthly mortgage × 12) + Σ annual tax/maintenance
 *                               − cumulative appreciation
 *   cumulative rent cost      = Σ annual rent
 *
 * The function checks each year from 1 to MAX_BREAK_EVEN_YEARS; returns the first
 * year where cumulative rent ≥ cumulative ownership.
 *
 * Assumptions:
 *   - Tax + maintenance: 0.3% of price per year (conservative for Taiwan)
 *   - Down payment is an opportunity cost (not subtracted — modelling invested capital
 *     is optional and not included to keep the model simple/conservative)
 *   - Price appreciates at `annualAppreciationPct` percent per year (compound)
 *
 * @param totalPriceNTD       Total property price in NT$
 * @param downPaymentPct      Down payment as percentage (0–100)
 * @param annualRatePercent   Annual mortgage rate percentage (e.g. 2.35)
 * @param termYears           Loan term in years
 * @param monthlyRentNTD      Estimated monthly rent in NT$
 * @param annualAppreciationPct  Annual price appreciation % (default 0)
 * @returns BreakEvenResult
 */
export function computeBreakEvenYears(
  totalPriceNTD: number,
  downPaymentPct: number,
  annualRatePercent: number,
  termYears: number,
  monthlyRentNTD: number,
  annualAppreciationPct = 0,
): BreakEvenResult {
  if (
    !Number.isFinite(totalPriceNTD) || totalPriceNTD <= 0 ||
    !Number.isFinite(downPaymentPct) || downPaymentPct < 0 || downPaymentPct > 100 ||
    !Number.isFinite(annualRatePercent) || annualRatePercent < 0 ||
    !Number.isFinite(termYears) || termYears <= 0 ||
    !Number.isFinite(monthlyRentNTD) || monthlyRentNTD <= 0
  ) {
    return { years: null, withinLoanTerm: false };
  }

  const loanPct = Math.max(0, Math.min(100, 100 - downPaymentPct));
  const principalNTD = totalPriceNTD * (loanPct / 100);
  const monthlyMortgage = computeMonthlyPayment(principalNTD, annualRatePercent, termYears);

  // Annual tax + maintenance: 0.3% of purchase price
  const annualOwnershipFixedCost = totalPriceNTD * 0.003;

  let cumulativeOwnership = 0;
  let cumulativeRent = 0;
  let propertyValue = totalPriceNTD;

  for (let year = 1; year <= MAX_BREAK_EVEN_YEARS; year++) {
    // Mortgage payment: 0 after loan term ends
    const annualMortgage = year <= termYears ? monthlyMortgage * 12 : 0;
    cumulativeOwnership += annualMortgage + annualOwnershipFixedCost;

    // Subtract appreciation: property gains value each year
    const appreciationThisYear = propertyValue * (annualAppreciationPct / 100);
    propertyValue += appreciationThisYear;
    // Net ownership cost is reduced by appreciation (gain on asset)
    cumulativeOwnership -= appreciationThisYear;

    cumulativeRent += monthlyRentNTD * 12;

    if (cumulativeRent >= cumulativeOwnership) {
      return {
        years: year,
        withinLoanTerm: year <= termYears,
      };
    }
  }

  return { years: null, withinLoanTerm: false };
}

/**
 * Suggest a monthly rent estimate based on city and floor area.
 *
 * @param cityName  Chinese city name (e.g. "台北市")
 * @param pingArea  Floor area in 坪 (Taiwan ping, ~3.3m²); defaults to 30
 * @returns Suggested monthly rent in NT$
 */
export function suggestMonthlyRent(cityName: string, pingArea = 30): number {
  const perPing = DEFAULT_RENT_PER_PING[cityName] ?? DEFAULT_RENT_PER_PING_FALLBACK;
  const area = Number.isFinite(pingArea) && pingArea > 0 ? pingArea : 30;
  return Math.round(perPing * area);
}

/**
 * Compute the full 租售比 summary from inputs.
 *
 * @param totalPriceNTD      Total property price in NT$
 * @param monthlyRentNTD     Estimated monthly rent in NT$
 * @param downPaymentPct     Down payment percentage (0–100)
 * @param annualRatePercent  Annual mortgage rate %
 * @param termYears          Loan term in years
 * @param annualAppreciationPct  Price appreciation % per year (default 0)
 */
export function computeRentRatioSummary(
  totalPriceNTD: number,
  monthlyRentNTD: number,
  downPaymentPct: number,
  annualRatePercent: number,
  termYears: number,
  annualAppreciationPct = 0,
): RentRatioSummary | null {
  if (
    !Number.isFinite(totalPriceNTD) || totalPriceNTD <= 0 ||
    !Number.isFinite(monthlyRentNTD) || monthlyRentNTD <= 0
  ) return null;

  const annualRentNTD = monthlyRentNTD * 12;
  const ratio = computePriceToRentRatio(totalPriceNTD, annualRentNTD);
  const tier = rentTierLabel(ratio);
  const breakEven = computeBreakEvenYears(
    totalPriceNTD,
    downPaymentPct,
    annualRatePercent,
    termYears,
    monthlyRentNTD,
    annualAppreciationPct,
  );

  const loanPct = Math.max(0, Math.min(100, 100 - downPaymentPct));
  const principalNTD = totalPriceNTD * (loanPct / 100);
  const monthlyMortgageNTD = computeMonthlyPayment(principalNTD, annualRatePercent, termYears);

  return {
    priceToRentRatio: ratio,
    tier,
    breakEven,
    monthlyMortgageNTD,
    monthlyRentNTD,
    rentingCheaper: monthlyRentNTD < monthlyMortgageNTD,
  };
}
