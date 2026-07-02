import { describe, it, expect } from "vitest";
import {
  computeMonthlyPayment,
  computeDaysRemaining,
  computeLoanComparison,
  formatNTD,
  formatWan,
  LOAN_PROGRAMS,
  type LoanProgram,
} from "../lib/loanProgramUtils";

// ── computeMonthlyPayment ─────────────────────────────────────────────────────

describe("computeMonthlyPayment", () => {
  it("returns positive payment for standard inputs", () => {
    const payment = computeMonthlyPayment(8_000_000, 2.35, 30);
    expect(payment).toBeGreaterThan(0);
  });

  it("lower rate → lower monthly payment", () => {
    const cheap = computeMonthlyPayment(8_000_000, 1.775, 30);
    const expensive = computeMonthlyPayment(8_000_000, 2.35, 30);
    expect(cheap).toBeLessThan(expensive);
  });

  it("rate=0 → principal divided by total months", () => {
    const principal = 6_000_000;
    const years = 30;
    const payment = computeMonthlyPayment(principal, 0, years);
    expect(payment).toBeCloseTo(principal / (years * 12), 2);
  });

  it("principal=0 → 0", () => {
    expect(computeMonthlyPayment(0, 2.35, 30)).toBe(0);
  });

  it("negative principal → 0", () => {
    expect(computeMonthlyPayment(-1_000_000, 2.35, 30)).toBe(0);
  });

  it("termYears=0 → 0", () => {
    expect(computeMonthlyPayment(5_000_000, 2.35, 0)).toBe(0);
  });

  it("negative rate → 0", () => {
    expect(computeMonthlyPayment(5_000_000, -1, 30)).toBe(0);
  });

  it("non-finite principal → 0", () => {
    expect(computeMonthlyPayment(Infinity, 2.35, 30)).toBe(0);
    expect(computeMonthlyPayment(NaN, 2.35, 30)).toBe(0);
  });

  it("20-year term has higher monthly than 30-year for same principal/rate", () => {
    const p20 = computeMonthlyPayment(8_000_000, 2.35, 20);
    const p30 = computeMonthlyPayment(8_000_000, 2.35, 30);
    expect(p20).toBeGreaterThan(p30);
  });
});

// ── computeDaysRemaining ──────────────────────────────────────────────────────

describe("computeDaysRemaining", () => {
  it("returns null for null deadline", () => {
    expect(computeDaysRemaining(null)).toBeNull();
  });

  it("returns null for malformed deadline date string", () => {
    expect(computeDaysRemaining("not-a-date")).toBeNull();
    expect(computeDaysRemaining("2026-99-99")).toBeNull();
    expect(computeDaysRemaining("2026-02-30")).toBeNull(); // impossible date, JS would normalize without strict check
  });

  it("returns positive days for future deadline", () => {
    const future = new Date("2024-01-01");
    const deadline = "2024-12-31";
    const days = computeDaysRemaining(deadline, future);
    expect(days).toBeGreaterThan(0);
  });

  it("returns null when deadline is past", () => {
    const past = new Date("2027-01-01");
    const days = computeDaysRemaining("2026-07-31", past);
    expect(days).toBeNull();
  });

  it("returns 1 on the day of deadline (same day, early morning)", () => {
    // Early morning Taiwan time on the deadline day
    const sameDay = new Date("2026-07-31T08:00:00+08:00");
    const days = computeDaysRemaining("2026-07-31", sameDay);
    expect(days).toBeGreaterThanOrEqual(1);
  });

  it("returns null just after deadline end-of-day Taiwan time", () => {
    // Just after 2026-07-31 23:59:59 Taiwan time
    const afterDeadline = new Date("2026-08-01T00:00:00+08:00");
    const days = computeDaysRemaining("2026-07-31", afterDeadline);
    expect(days).toBeNull();
  });
});

// ── computeLoanComparison ─────────────────────────────────────────────────────

describe("computeLoanComparison", () => {
  it("returns one entry per program", () => {
    const result = computeLoanComparison(1000, 20);
    expect(result).toHaveLength(LOAN_PROGRAMS.length);
  });

  it("principal is correctly derived from price and down payment", () => {
    // 1000 萬 * 80% = 8,000,000 NT$
    const result = computeLoanComparison(1000, 20);
    const nonTbd = result.filter((r) => !r.program.rateTBD);
    for (const r of nonTbd) {
      expect(r.principalNTD).toBeCloseTo(8_000_000, 0);
    }
  });

  it("total interest = totalPaid - principal (non-TBD programs)", () => {
    const result = computeLoanComparison(1000, 20);
    for (const r of result.filter((r) => !r.program.rateTBD)) {
      expect(r.totalInterestNTD).toBeCloseTo(r.totalPaidNTD - r.principalNTD, -1);
    }
  });

  it("civil servant rate is cheapest (lowest rate among non-TBD)", () => {
    // Use explicit date well before 青安 deadline so it is eligible too
    const before = new Date("2025-01-01");
    const result = computeLoanComparison(1000, 20, LOAN_PROGRAMS, before);
    const cheapest = result.find((r) => r.isCheapest);
    expect(cheapest?.program.id).toBe("civil-servant");
  });

  it("exactly one entry is marked isCheapest (non-zero principal)", () => {
    const result = computeLoanComparison(1000, 20);
    const cheapestCount = result.filter((r) => r.isCheapest).length;
    expect(cheapestCount).toBe(1);
  });

  it("TBD program has monthlyPaymentNTD = 0", () => {
    const result = computeLoanComparison(1000, 20);
    const tbd = result.find((r) => r.program.rateTBD);
    expect(tbd?.monthlyPaymentNTD).toBe(0);
  });

  it("100% down payment → zero principal → all payments are 0", () => {
    const result = computeLoanComparison(1000, 100);
    for (const r of result) {
      expect(r.principalNTD).toBe(0);
      expect(r.monthlyPaymentNTD).toBe(0);
      expect(r.totalPaidNTD).toBe(0);
    }
  });

  it("zero down payment → principal equals full price", () => {
    const result = computeLoanComparison(1000, 0);
    for (const r of result.filter((r) => !r.program.rateTBD)) {
      expect(r.principalNTD).toBeCloseTo(10_000_000, 0); // 1000 萬 = 10,000,000 NT$
    }
  });

  it("returns zero-result rows for invalid totalPriceWan", () => {
    const result = computeLoanComparison(-1, 20);
    for (const r of result) {
      expect(r.monthlyPaymentNTD).toBe(0);
    }
  });

  it("returns zero-result rows for NaN inputs", () => {
    const result = computeLoanComparison(NaN, NaN);
    for (const r of result) {
      expect(r.monthlyPaymentNTD).toBe(0);
    }
  });

  it("downPaymentPct > 100 returns zero principal", () => {
    const result = computeLoanComparison(1000, 120);
    for (const r of result) {
      expect(r.principalNTD).toBe(0);
    }
  });

  it("isCheapest is false for all rows when all payments are zero", () => {
    const result = computeLoanComparison(1000, 100);
    expect(result.every((r) => !r.isCheapest)).toBe(true);
  });

  it("custom programs array is respected", () => {
    const custom: LoanProgram[] = [
      {
        id: "custom",
        name: "自訂方案",
        ratePercent: 3.0,
        termYears: 20,
        eligibilityNote: "測試",
        deadlineDate: null,
        rateTBD: false,
      },
    ];
    const result = computeLoanComparison(800, 20, custom);
    expect(result).toHaveLength(1);
    expect(result[0].program.id).toBe("custom");
    expect(result[0].isCheapest).toBe(true); // only one non-TBD program
  });

  it("expired 青安 is not marked cheapest after its deadline", () => {
    // After 青安 expires, civil servant (1.36%) should be cheapest
    const after = new Date("2026-08-01T00:00:00+08:00");
    const result = computeLoanComparison(1000, 20, LOAN_PROGRAMS, after);
    const qingan = result.find((r) => r.program.id === "qingan");
    const civilServant = result.find((r) => r.program.id === "civil-servant");
    expect(qingan?.isCheapest).toBe(false);
    expect(civilServant?.isCheapest).toBe(true);
  });

  it("daysRemaining is null for programs without a deadline", () => {
    const result = computeLoanComparison(1000, 20);
    const general = result.find((r) => r.program.id === "general");
    expect(general?.daysRemaining).toBeNull();
  });

  it("daysRemaining is positive for 青安 when queried well before 2026-07-31", () => {
    const early = new Date("2026-01-01");
    const result = computeLoanComparison(1000, 20, LOAN_PROGRAMS, early);
    const qingan = result.find((r) => r.program.id === "qingan");
    expect(qingan?.daysRemaining).toBeGreaterThan(0);
  });

  it("daysRemaining is null for 青安 after deadline", () => {
    const after = new Date("2026-08-01T00:00:00+08:00");
    const result = computeLoanComparison(1000, 20, LOAN_PROGRAMS, after);
    const qingan = result.find((r) => r.program.id === "qingan");
    expect(qingan?.daysRemaining).toBeNull();
  });

  it("totalInterest is non-negative for all programs", () => {
    const result = computeLoanComparison(1000, 20);
    for (const r of result) {
      expect(r.totalInterestNTD).toBeGreaterThanOrEqual(0);
    }
  });
});

// ── LOAN_PROGRAMS constants ───────────────────────────────────────────────────

describe("LOAN_PROGRAMS", () => {
  it("contains at least 3 programs", () => {
    expect(LOAN_PROGRAMS.length).toBeGreaterThanOrEqual(3);
  });

  it("all program IDs are unique", () => {
    const ids = LOAN_PROGRAMS.map((p) => p.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("non-TBD programs have ratePercent > 0", () => {
    for (const p of LOAN_PROGRAMS.filter((p) => !p.rateTBD)) {
      expect(p.ratePercent).toBeGreaterThan(0);
    }
  });

  it("青安 program has deadlineDate set", () => {
    const qingan = LOAN_PROGRAMS.find((p) => p.id === "qingan");
    expect(qingan?.deadlineDate).not.toBeNull();
  });
});

// ── formatNTD / formatWan ─────────────────────────────────────────────────────

describe("formatNTD", () => {
  it("formats a round number with 元 suffix", () => {
    expect(formatNTD(12345)).toContain("元");
    expect(formatNTD(12345)).toContain("12,345");
  });

  it("returns fallback for non-finite input", () => {
    expect(formatNTD(Infinity)).toContain("—");
    expect(formatNTD(NaN)).toContain("—");
  });
});

describe("formatWan", () => {
  it("converts NT$ to 萬 with one decimal", () => {
    const result = formatWan(1_230_000);
    expect(result).toContain("123.0");
    expect(result).toContain("萬");
  });

  it("returns fallback for non-finite input", () => {
    expect(formatWan(Infinity)).toContain("—");
  });
});
