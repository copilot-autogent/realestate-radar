/**
 * Pure utility functions for building age distribution chart (issue #134).
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * IMPORTANT: Taiwan gov data stores 建築完成年月 as ROC date integers (e.g. 741024 = ROC 74/10/24
 * = Western 1985/10/24) and may use full-width CJK digits when represented as strings.
 * parseBuildYear() handles all forms: NFKC-normalised strings, YYMMDD/YYYMMDD integers,
 * plain ROC years (< 200), and Western 4-digit years.
 */

export interface AgeBucket {
  /** Human-readable label, e.g. "< 5年" */
  label: string;
  /** Inclusive lower bound in years */
  minAge: number;
  /** Exclusive upper bound in years (Infinity for the last bucket) */
  maxAge: number;
  /** Transaction count in this bucket */
  count: number;
  /** Percentage of transactions with year data that fall in this bucket (0–100) */
  pct: number;
  /**
   * Corresponding value for the <select id="build-age-range"> filter in index.astro.
   * Clicking the bar should set the filter to this value.
   */
  filterValue: "0-5" | "5-20" | "20-40" | "40+";
}

export interface BuildingAgeResult {
  districtId: string;
  buckets: AgeBucket[];
  /** True when ≥ 5 transactions have year data — chart should render */
  sufficient: boolean;
  /**
   * Percentage of district transactions that have a parseable buildYear (0–100).
   * Displayed as "資料完整度: N%".
   */
  completenessPercent: number;
  /**
   * Risk/opportunity annotation:
   * "urban-renewal-risk" → 🟡 都更風險 (> 40% of year-bearing transactions in > 30-year buckets)
   * "mostly-new"         → 🟢 新屋為主 (> 50% of year-bearing transactions in < 10-year buckets)
   * null                 → no badge
   */
  annotation: "urban-renewal-risk" | "mostly-new" | null;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

const BUCKET_DEFS: ReadonlyArray<{
  label: string;
  minAge: number;
  maxAge: number;
  filterValue: AgeBucket["filterValue"];
}> = [
  { label: "< 5年",    minAge: 0,        maxAge: 5,        filterValue: "0-5"   },
  { label: "5–10年",   minAge: 5,        maxAge: 10,       filterValue: "5-20"  },
  { label: "10–20年",  minAge: 10,       maxAge: 20,       filterValue: "5-20"  },
  { label: "20–30年",  minAge: 20,       maxAge: 30,       filterValue: "20-40" },
  { label: "30–40年",  minAge: 30,       maxAge: 40,       filterValue: "20-40" },
  { label: "> 40年",   minAge: 40,       maxAge: Infinity, filterValue: "40+"   },
];

/**
 * Parse a raw buildYear value from a GeoJSON feature property into a Western calendar year.
 *
 * Handles:
 *  - null / undefined / falsy → null
 *  - String values: NFKC-normalise (full-width → ASCII) then parseInt
 *  - 6–7-digit integers (ROC YYMMDD or YYYMMDD, e.g. 741024 or 1081128):
 *      extract year portion = Math.floor(n / 10000); treat as ROC year
 *  - 1–3-digit integers (plain ROC year, e.g. 74): add 1911
 *  - 4-digit integers ≥ 1900 (Western year): use as-is
 *
 * Returns null on any parse failure or implausible result.
 */
export function parseBuildYear(raw: unknown): number | null {
  if (raw === null || raw === undefined || raw === "" || raw === false) return null;

  let n: number;

  if (typeof raw === "string") {
    // NFKC normalises full-width CJK digits (U+FF10–FF19) to ASCII digits
    const s = raw.normalize("NFKC").trim();
    if (s === "") return null;
    n = parseInt(s, 10);
  } else if (typeof raw === "number") {
    n = raw;
  } else {
    return null;
  }

  if (!isFinite(n) || isNaN(n) || n <= 0) return null;

  let year: number;

  if (n >= 100_000) {
    // 6–7 digit: YYMMDD or YYYMMDD  (ROC date integer from 建築完成年月)
    const rocYear = Math.floor(n / 10_000);
    year = rocYear + 1911;
  } else if (n < 200) {
    // 1–3 digit: plain ROC year
    year = n + 1911;
  } else if (n >= 1900 && n <= 2200) {
    // 4-digit Western year
    year = n;
  } else {
    return null;
  }

  // Sanity: building years between 1900 and current year + 1 are plausible
  if (year < 1900 || year > new Date().getFullYear() + 1) return null;
  return year;
}

/**
 * Compute the age in years from a Western build year using the reference year.
 * Returns null if buildYear is null or the computed age is implausible.
 */
export function buildYearToAge(buildYear: number | null, referenceYear: number): number | null {
  if (buildYear === null) return null;
  const age = referenceYear - buildYear;
  if (age < 0 || age >= 200) return null;
  return age;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Compute the building age distribution for a given district.
 *
 * @param features    GeoJSON feature array (rawAllFeatures from Map.astro).
 *                    Each feature must have properties: { district, city?, buildYear? }.
 * @param districtId  District name to filter (matches feature.properties.district).
 * @param city        Optional city to disambiguate same-name districts.
 * @param referenceYear  Year to use when computing age (default: current calendar year).
 */
export function computeBuildingAgeDistribution(
  features: any[],
  districtId: string,
  city?: string,
  referenceYear?: number,
): BuildingAgeResult {
  const refYear = referenceYear ?? new Date().getFullYear();

  // Filter to the requested district (and optional city)
  const districtFeatures = features.filter((f) => {
    const p = f?.properties ?? f;
    if ((p.district as string | undefined) !== districtId) return false;
    if (city && p.city && (p.city as string) !== city) return false;
    return true;
  });

  const totalCount = districtFeatures.length;

  // Compute age for each feature; track which ones have parseable years
  const ages: number[] = [];
  for (const f of districtFeatures) {
    const p = f?.properties ?? f;
    const rawBy = p.buildYear ?? p.buildingCompletionYear ?? null;
    const buildYear = parseBuildYear(rawBy);
    const age = buildYearToAge(buildYear, refYear);
    if (age !== null) ages.push(age);
  }

  const withYearCount = ages.length;
  const completenessPercent =
    totalCount === 0 ? 0 : Math.round((withYearCount / totalCount) * 100);

  const sufficient = withYearCount >= 5;

  // Fill buckets
  const counts = BUCKET_DEFS.map(() => 0);
  for (const age of ages) {
    for (let i = 0; i < BUCKET_DEFS.length; i++) {
      const b = BUCKET_DEFS[i]!;
      if (age >= b.minAge && age < b.maxAge) {
        counts[i]!++;
        break;
      }
    }
  }

  const buckets: AgeBucket[] = BUCKET_DEFS.map((def, i) => {
    const count = counts[i]!;
    const pct = withYearCount === 0 ? 0 : Math.round((count / withYearCount) * 100);
    return { ...def, count, pct };
  });

  // Annotation: >40% in >30年 (buckets 30-40 + >40) → 都更風險
  //             >50% in <10年 (buckets <5 + 5-10)    → 新屋為主
  let annotation: BuildingAgeResult["annotation"] = null;
  if (sufficient) {
    const over30Count = (counts[4] ?? 0) + (counts[5] ?? 0);
    const under10Count = (counts[0] ?? 0) + (counts[1] ?? 0);
    if (over30Count / withYearCount > 0.4) {
      annotation = "urban-renewal-risk";
    } else if (under10Count / withYearCount > 0.5) {
      annotation = "mostly-new";
    }
  }

  return { districtId, buckets, sufficient, completenessPercent, annotation };
}
