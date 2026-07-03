/**
 * Pure utility functions for quarterly transaction-volume YoY comparison (issue #132).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

export interface QuarterlyVolume {
  quarter: Quarter;
  /** Transaction count in the reference (current) year */
  currentYear: number;
  /** Transaction count in the prior year (referenceYear − 1) */
  priorYear: number;
  /**
   * YoY percentage change: ((currentYear − priorYear) / priorYear) × 100.
   * Null when priorYear is 0 (division by zero).
   */
  yoyPct: number | null;
  /** true when currentYear count < 4 (insufficient data for the quarter) */
  insufficientCurrent: boolean;
  /** true when priorYear count < 4 (insufficient data for the prior quarter) */
  insufficientPrior: boolean;
}

/** YTD totals derived from QuarterlyVolume[] */
export interface YTDSummary {
  ytdCurrent: number;
  ytdPrior: number;
  /**
   * ((ytdCurrent − ytdPrior) / ytdPrior) × 100.
   * Null when ytdPrior is 0.
   */
  ytdYoYPct: number | null;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

const QUARTER_MONTHS: Record<Quarter, number[]> = {
  Q1: [1, 2, 3],
  Q2: [4, 5, 6],
  Q3: [7, 8, 9],
  Q4: [10, 11, 12],
};

/** Quarter ordinal (1-based) for month ordering */
const QUARTER_INDEX: Record<Quarter, number> = { Q1: 1, Q2: 2, Q3: 3, Q4: 4 };

/** Map month (1–12) → quarter label */
function monthToQuarter(month: number): Quarter {
  if (month <= 3) return "Q1";
  if (month <= 6) return "Q2";
  if (month <= 9) return "Q3";
  return "Q4";
}

/** Returns the current quarter (1–4) for a given month (1–12) */
function quarterIndex(month: number): number {
  return Math.ceil(month / 3);
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Parse the year and month from a raw date string (or Date object).
 * Accepts "YYYY-MM-DD", "YYYY/MM/DD", ISO, and "YYYYMMDD" formats.
 * Normalises with NFKC before regex to handle full-width CJK digits (U+FF10–FF19).
 *
 * Returns { year, month } (both 1-based integers) or null on failure.
 */
export function parseTransactionDate(raw: unknown): { year: number; month: number } | null {
  if (raw instanceof Date) {
    if (isNaN(raw.getTime())) return null;
    return { year: raw.getFullYear(), month: raw.getMonth() + 1 };
  }
  if (!raw || typeof raw !== "string") return null;

  // NFKC normalisation converts full-width digits to ASCII
  const s = raw.normalize("NFKC");

  // YYYY-MM-DD or YYYY/MM/DD or YYYY-M-D (end-anchored to reject garbage after date)
  const m = /^(\d{4})[-/](\d{1,2})(?:[-/]\d{1,2})?(?:[T\s]|$)/.exec(s);
  if (m) {
    const year = parseInt(m[1]!, 10);
    const month = parseInt(m[2]!, 10);
    if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
      return null;
    }
    return { year, month };
  }

  // YYYYMMDD compact form
  const m2 = /^(\d{4})(\d{2})(\d{2})$/.exec(s);
  if (m2) {
    const year = parseInt(m2[1]!, 10);
    const month = parseInt(m2[2]!, 10);
    if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
      return null;
    }
    return { year, month };
  }

  return null;
}

/**
 * Compute per-quarter transaction-volume counts for two consecutive years.
 *
 * @param features       GeoJSON feature array (each feature.properties has .date, .district, .city)
 * @param districtId     District name to filter on (NFKC-normalised comparison)
 * @param referenceYear  The "current" calendar year (prior year = referenceYear − 1)
 * @param city           Optional city filter
 * @returns              Array of 4 QuarterlyVolume objects (Q1–Q4), always all 4 quarters present
 */
export function computeVolumeYoY(
  features: any[],
  districtId: string,
  referenceYear: number,
  city?: string,
): QuarterlyVolume[] {
  const normDistrict = districtId.normalize("NFKC");
  const normCity     = city?.normalize("NFKC");
  const priorYear    = referenceYear - 1;

  // Count transactions by year × quarter
  const counts: Record<Quarter, { current: number; prior: number }> = {
    Q1: { current: 0, prior: 0 },
    Q2: { current: 0, prior: 0 },
    Q3: { current: 0, prior: 0 },
    Q4: { current: 0, prior: 0 },
  };

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;

    if (typeof p.district !== "string") continue;
    if (p.district.normalize("NFKC") !== normDistrict) continue;

    if (normCity) {
      if (typeof p.city !== "string" || p.city.normalize("NFKC") !== normCity) continue;
    }

    // date may be a raw string or already a Date-like; accept both
    const parsed = parseTransactionDate(p.date ?? p.transactionDate);
    if (!parsed) continue;

    const { year, month } = parsed;
    if (year !== referenceYear && year !== priorYear) continue;

    const q = monthToQuarter(month);
    if (year === referenceYear) {
      counts[q].current += 1;
    } else {
      counts[q].prior += 1;
    }
  }

  return (["Q1", "Q2", "Q3", "Q4"] as const).map((q) => {
    const { current, prior } = counts[q];
    const yoyPct =
      prior > 0 ? Math.round(((current - prior) / prior) * 1000) / 10 : null;
    return {
      quarter: q,
      currentYear: current,
      priorYear: prior,
      yoyPct,
      insufficientCurrent: current < 4,
      insufficientPrior:   prior  < 4,
    };
  });
}

/**
 * Compute YTD (year-to-date) totals from a QuarterlyVolume array.
 *
 * For an in-progress year, only quarters up to the current calendar quarter are
 * included in the YTD comparison. This avoids inflating `ytdPrior` with future
 * prior-year quarters whose current-year equivalents haven't happened yet.
 *
 * Pass `currentMonth` (1–12) to enable the mid-year cutoff. Defaults to all
 * 4 quarters (useful for completed years or when comparing full-year figures).
 */
export function computeYTDSummary(
  volumes: QuarterlyVolume[],
  currentMonth = 12,
): YTDSummary {
  // Include only quarters that have started (Q1 starts in month 1, Q2 in month 4, etc.)
  const activeMaxQuarter = Math.ceil(currentMonth / 3); // 1–4
  let ytdCurrent = 0;
  let ytdPrior   = 0;
  for (const v of volumes) {
    const qi = QUARTER_INDEX[v.quarter];
    if (qi > activeMaxQuarter) continue; // future quarter — exclude
    ytdCurrent += v.currentYear;
    ytdPrior   += v.priorYear;
  }
  const ytdYoYPct =
    ytdPrior > 0 ? Math.round(((ytdCurrent - ytdPrior) / ytdPrior) * 1000) / 10 : null;
  return { ytdCurrent, ytdPrior, ytdYoYPct };
}

/**
 * Format a YoY percentage for display.
 * e.g.  12.3 → "+12.3%"   -28 → "-28%"   null → "—"
 */
export function formatYoYPct(pct: number | null): string {
  if (pct === null) return "—";
  const sign = pct >= 0 ? "+" : "-";
  const abs  = Math.abs(pct);
  const str  = Number.isInteger(abs) ? String(abs) : abs.toFixed(1);
  return `${sign}${str}%`;
}

// Export QUARTER_MONTHS for consumers that need the month membership mapping
export { QUARTER_MONTHS };
