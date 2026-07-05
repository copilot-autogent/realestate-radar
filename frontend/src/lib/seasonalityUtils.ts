/**
 * Pure utility functions for monthly seasonality pattern analysis (issue #142).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

/** Current-month seasonality tier based on the price seasonality index. */
export type SeasonalityTier = "low" | "mid" | "high";

export interface SeasonalityResult {
  /** Average transaction count for each calendar month (index 0 = January). */
  volumeByMonth: number[];
  /**
   * Price seasonality index for each calendar month (index 0 = January).
   * Ratio of that month's median unit-price-per-ping vs the annual average × 100.
   * A value of 100 means average; <100 = historically cheaper; >100 = historically pricier.
   * Null when insufficient per-ping data exists for a given month.
   */
  priceIndexByMonth: (number | null)[];
  /**
   * Seasonality tier for the current calendar month (at call time).
   * "low"  → price index < 95 (淡季, historically cheaper)
   * "mid"  → price index 95–105 (平季, average)
   * "high" → price index > 105 (旺季, historically pricier)
   * Null when the current month has insufficient price data.
   */
  currentMonthTier: SeasonalityTier | null;
  /** Count of distinct (year, month) pairs with transaction data for the district. */
  minTxMonths: number;
  /** Count of distinct calendar years with transaction data for the district. */
  distinctYears: number;
  /**
   * Annual average of the monthly volume averages.
   * Used to identify "buyer window" months (volume < 70% of annualVolumeAvg).
   */
  annualVolumeAvg: number;
  /**
   * false when fewer than 3 distinct years of data are available.
   * The panel must show 「資料不足」when sufficient is false.
   */
  sufficient: boolean;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/** Sort array ascending; returns a new array. */
function sortedNums(arr: number[]): number[] {
  return [...arr].sort((a, b) => a - b);
}

/** Median of a sorted ascending array. Returns 0 for empty input. */
function median(sorted: number[]): number {
  if (sorted.length === 0) return 0;
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Parse the calendar month (1–12) and year from a raw date string.
 * Accepts YYYY-MM-DD, YYYY/MM/DD, YYYY-M-DD, and YYYY-MM formats.
 * Normalises with NFKC before regex to handle full-width CJK digits (U+FF10–FF19).
 *
 * Returns { year, month } or null on any parse failure.
 */
export function parseDateYearMonth(raw: unknown): { year: number; month: number } | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  const m = /^(\d{4})[-/](\d{1,2})(?:[-/\s]|$)/.exec(s);
  if (!m) return null;
  const year  = parseInt(m[1]!, 10);
  const month = parseInt(m[2]!, 10);
  if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
    return null;
  }
  return { year, month };
}

/**
 * Compute monthly seasonality metrics for a district.
 *
 * @param features   GeoJSON feature array (rawAllFeatures from Map.astro / window.__TRANSACTIONS__)
 * @param districtId District name to filter on (e.g. "大安區")
 * @param city       Optional city filter (e.g. "台北市")
 * @param nowMonth   1-based current calendar month (defaults to current system month).
 *                   Injectable for deterministic testing.
 * @returns SeasonalityResult — sufficient = false when < 3 distinct years of data
 */
export function computeMonthlySeasonality(
  features: any[],
  districtId: string,
  city?: string,
  nowMonth?: number,
): SeasonalityResult {
  const EMPTY: SeasonalityResult = {
    volumeByMonth: new Array(12).fill(0),
    priceIndexByMonth: new Array(12).fill(null),
    currentMonthTier: null,
    minTxMonths: 0,
    distinctYears: 0,
    annualVolumeAvg: 0,
    sufficient: false,
  };

  if (!features || !features.length || !districtId) return EMPTY;

  const normDistrict = districtId.normalize("NFKC");
  const normCity     = city?.normalize("NFKC");

  // Accumulate per (year, month) key → list of unit prices for that bucket
  const yrMoVolume = new Map<string, number>();    // key → transaction count
  const moPrices   = new Map<number, number[]>();  // month (1–12) → unit prices
  const yearsSet   = new Set<number>();

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;

    if (typeof p.district !== "string") continue;
    if (p.district.normalize("NFKC") !== normDistrict) continue;

    if (normCity) {
      if (typeof p.city !== "string" || p.city.normalize("NFKC") !== normCity) continue;
    }

    const parsed = parseDateYearMonth(p.date);
    if (!parsed) continue;

    const { year, month } = parsed;
    const key = `${year}-${String(month).padStart(2, "0")}`;

    // Volume accumulation
    yrMoVolume.set(key, (yrMoVolume.get(key) ?? 0) + 1);
    yearsSet.add(year);

    // Price accumulation (only when unitPrice is valid)
    const up = Number(p.unitPrice);
    if (isFinite(up) && up > 0) {
      if (!moPrices.has(month)) moPrices.set(month, []);
      moPrices.get(month)!.push(up);
    }
  }

  const distinctYears = yearsSet.size;
  const minTxMonths   = yrMoVolume.size;

  // Require at least 3 distinct calendar years AND 12 (year, month) pairs
  // for a meaningful seasonality signal. 3 years × 1 month = 3 pairs is not enough.
  if (distinctYears < 3 || minTxMonths < 12) {
    return { ...EMPTY, minTxMonths, distinctYears };
  }

  // ── Volume by month ───────────────────────────────────────────────────────
  // Aggregate per calendar month across all (year, month) pairs.
  const monthVolumeBuckets: number[][] = Array.from({ length: 12 }, () => []);
  for (const [key, count] of yrMoVolume) {
    const mo = parseInt(key.slice(5, 7), 10) - 1; // 0-indexed
    if (mo >= 0 && mo < 12) monthVolumeBuckets[mo]!.push(count);
  }
  const volumeByMonth = monthVolumeBuckets.map(bucket =>
    bucket.length > 0 ? bucket.reduce((a, b) => a + b, 0) / bucket.length : 0,
  );

  // Count-weighted annual average (SUM(all yr-mo counts) / COUNT(yr-mo pairs))
  const totalCount = [...yrMoVolume.values()].reduce((a, b) => a + b, 0);
  const annualVolumeAvg = minTxMonths > 0 ? totalCount / minTxMonths : 0;

  // ── Price seasonality index ───────────────────────────────────────────────
  // For each month, compute median unit-price-per-ping across all years.
  // Then express each as a ratio to the annual mean × 100.

  // Monthly median unit prices
  const monthMedians: (number | null)[] = Array.from({ length: 12 }, (_, i) => {
    const prices = moPrices.get(i + 1); // month is 1-indexed
    if (!prices || prices.length === 0) return null;
    return median(sortedNums(prices));
  });

  // Annual average of the valid monthly medians
  const validMedians = monthMedians.filter((v): v is number => v !== null);
  const annualMedianAvg = validMedians.length > 0
    ? validMedians.reduce((a, b) => a + b, 0) / validMedians.length
    : 0;

  const priceIndexByMonth: (number | null)[] = monthMedians.map(v => {
    if (v === null || annualMedianAvg === 0) return null;
    return Math.round((v / annualMedianAvg) * 100 * 10) / 10; // 1 decimal
  });

  // ── Current-month tier ───────────────────────────────────────────────────
  const currentMonthIdx = ((nowMonth != null ? nowMonth : new Date().getMonth() + 1) - 1 + 12) % 12;
  const currentIndex = priceIndexByMonth[currentMonthIdx];
  let currentMonthTier: SeasonalityTier | null = null;
  if (currentIndex !== null) {
    if (currentIndex < 95) {
      currentMonthTier = "low";
    } else if (currentIndex > 105) {
      currentMonthTier = "high";
    } else {
      currentMonthTier = "mid";
    }
  }

  return {
    volumeByMonth,
    priceIndexByMonth,
    currentMonthTier,
    minTxMonths,
    distinctYears,
    annualVolumeAvg,
    sufficient: true,
  };
}
