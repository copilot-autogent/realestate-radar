/**
 * Pure utility functions for the loan program comparator (issue #125).
 * No DOM / window dependencies — fully testable with Vitest.
 *
 * Compares 3–4 Taiwan government / market mortgage programs side-by-side:
 *   - 青安貸款 (Youth Home Loan, ending 2026-07-31)
 *   - 一般房貸 (General market mortgage)
 *   - 公教人員優惠貸款 (Civil servant preferential loan)
 *   - 新青安2.0 (Next-gen Youth Loan, TBD)
 *
 * Inputs are in 萬 NT$ (1 萬 = NT$10,000); outputs are in NT$.
 */

// ── Types ────────────────────────────────────────────────────────────────────

export interface LoanProgram {
  /** Unique identifier */
  id: string;
  /** Display name (Chinese) */
  name: string;
  /** Annual interest rate as a percentage (e.g. 1.775 for 1.775%) */
  ratePercent: number;
  /** Standard loan term in years */
  termYears: number;
  /** Short eligibility note shown in the table */
  eligibilityNote: string;
  /** ISO 8601 date string (YYYY-MM-DD) for when the program ends; null = no deadline */
  deadlineDate: string | null;
  /** Whether this program has a TBD/announced-only rate (rates may change) */
  rateTBD: boolean;
}

export interface LoanComparison {
  program: LoanProgram;
  /** Loan principal in NT$ (price × (1 − downPaymentPct/100)) */
  principalNTD: number;
  /** Monthly amortized payment in NT$ */
  monthlyPaymentNTD: number;
  /** Total amount paid over full term (NT$) */
  totalPaidNTD: number;
  /** Total interest paid over full term (NT$) */
  totalInterestNTD: number;
  /** Whether this program is highlighted as cheapest non-TBD eligible option */
  isCheapest: boolean;
  /** Days remaining until program deadline; null when no deadline or already past */
  daysRemaining: number | null;
}

// ── Constants ────────────────────────────────────────────────────────────────

/** Taiwan standard loan programs. Update ratePercent here when policy changes. */
export const LOAN_PROGRAMS: LoanProgram[] = [
  {
    id: "qingan",
    name: "青安貸款",
    ratePercent: 1.775,
    termYears: 30,
    eligibilityNote: "首購族（資格審查）",
    deadlineDate: "2026-07-31",
    rateTBD: false,
  },
  {
    id: "general",
    name: "一般房貸",
    ratePercent: 2.35,
    termYears: 30,
    eligibilityNote: "市場均值，各行庫不同",
    deadlineDate: null,
    rateTBD: false,
  },
  {
    id: "civil-servant",
    name: "公教優惠貸款",
    ratePercent: 1.36,
    termYears: 30,
    eligibilityNote: "公務員 / 教師限定",
    deadlineDate: null,
    rateTBD: false,
  },
  {
    id: "qingan2",
    name: "新青安2.0",
    ratePercent: 0, // TBD — awaiting policy announcement
    termYears: 30,
    eligibilityNote: "政策宣告中，細節待定",
    deadlineDate: null,
    rateTBD: true,
  },
];

// ── Core calculation ──────────────────────────────────────────────────────────

/**
 * Compute the monthly amortized mortgage payment (等額攤還).
 *
 * @param principalNTD  Loan principal in NT$
 * @param ratePercent   Annual interest rate as a percentage (e.g. 2.5 for 2.5%)
 * @param termYears     Loan term in years
 * @returns Monthly payment in NT$; 0 when principal/term is 0 or negative
 */
export function computeMonthlyPayment(
  principalNTD: number,
  ratePercent: number,
  termYears: number
): number {
  if (!Number.isFinite(principalNTD) || principalNTD <= 0) return 0;
  if (!Number.isFinite(termYears) || termYears <= 0) return 0;
  if (!Number.isFinite(ratePercent) || ratePercent < 0) return 0;

  const n = termYears * 12;
  if (ratePercent === 0) return principalNTD / n;

  const r = ratePercent / 100 / 12;
  return principalNTD * (r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);
}

/**
 * Compute days remaining until `deadlineDate` (inclusive of the deadline day),
 * based on device clock.
 *
 * Returns null when deadlineDate is null.
 * Returns null when deadlineDate is malformed or unparseable.
 * Returns null (past) when the deadline has already passed (Taiwan time UTC+8).
 */
export function computeDaysRemaining(
  deadlineDate: string | null,
  now: Date = new Date()
): number | null {
  if (!deadlineDate) return null;

  // Interpret deadline as end-of-day Taiwan time (UTC+8 = UTC+480min)
  const deadline = new Date(`${deadlineDate}T23:59:59+08:00`);
  if (isNaN(deadline.getTime())) return null; // malformed date string
  const diffMs = deadline.getTime() - now.getTime();
  if (diffMs <= 0) return null; // already past
  return Math.ceil(diffMs / (1000 * 60 * 60 * 24));
}

/**
 * Main entry point: compute side-by-side loan comparisons for all LOAN_PROGRAMS.
 *
 * @param totalPriceWan   Total property price in 萬 NT$ (must be > 0)
 * @param downPaymentPct  Down payment as a percentage (0–100)
 * @param programs        Loan programs to compare (defaults to LOAN_PROGRAMS)
 * @param now             Current date for deadline calculation (injectable for testing)
 * @returns Array of LoanComparison, one per program, in LOAN_PROGRAMS order
 */
export function computeLoanComparison(
  totalPriceWan: number,
  downPaymentPct: number,
  programs: LoanProgram[] = LOAN_PROGRAMS,
  now: Date = new Date()
): LoanComparison[] {
  if (
    !Number.isFinite(totalPriceWan) ||
    totalPriceWan < 0 ||
    !Number.isFinite(downPaymentPct) ||
    downPaymentPct < 0 ||
    downPaymentPct > 100
  ) {
    return programs.map((p) => ({
      program: p,
      principalNTD: 0,
      monthlyPaymentNTD: 0,
      totalPaidNTD: 0,
      totalInterestNTD: 0,
      isCheapest: false,
      daysRemaining: computeDaysRemaining(p.deadlineDate, now),
    }));
  }

  const totalPriceNTD = totalPriceWan * 10_000;
  const loanPct = Math.max(0, Math.min(100, 100 - downPaymentPct));
  const principalNTD = totalPriceNTD * (loanPct / 100);

  const results: LoanComparison[] = programs.map((p) => {
    const monthly = p.rateTBD
      ? 0
      : computeMonthlyPayment(principalNTD, p.ratePercent, p.termYears);
    const totalPaid = monthly * p.termYears * 12;
    const totalInterest = Math.max(0, totalPaid - principalNTD);
    const daysRemaining = computeDaysRemaining(p.deadlineDate, now);

    return {
      program: p,
      principalNTD,
      monthlyPaymentNTD: monthly,
      totalPaidNTD: totalPaid,
      totalInterestNTD: totalInterest,
      isCheapest: false, // filled below
      daysRemaining,
    };
  });

  // Mark cheapest: lowest monthly payment among non-TBD programs with principal > 0
  // that have not passed their deadline (expired programs are no longer accessible).
  // A program with no deadline (deadlineDate === null) is always considered active.
  const eligible = results.filter(
    (r) => !r.program.rateTBD && r.monthlyPaymentNTD > 0 &&
           (r.program.deadlineDate === null || r.daysRemaining !== null)
  );
  if (eligible.length > 0) {
    const minPayment = Math.min(...eligible.map((r) => r.monthlyPaymentNTD));
    // Only mark a program cheapest if it is in the eligible set (non-TBD, active deadline)
    for (const r of eligible) {
      if (Math.abs(r.monthlyPaymentNTD - minPayment) < 0.01) {
        r.isCheapest = true;
        break; // only mark one (first tie wins)
      }
    }
  }

  return results;
}

// ── Formatting helpers ────────────────────────────────────────────────────────

/** Format NT$ amount with thousands separators: "12,345 元" */
export function formatNTD(amount: number): string {
  if (!Number.isFinite(amount)) return "— 元";
  return Math.round(amount).toLocaleString("zh-TW") + " 元";
}

/** Format NT$ as 萬: "123.4 萬" */
export function formatWan(amountNTD: number): string {
  if (!Number.isFinite(amountNTD)) return "— 萬";
  return (amountNTD / 10_000).toFixed(1) + " 萬";
}
