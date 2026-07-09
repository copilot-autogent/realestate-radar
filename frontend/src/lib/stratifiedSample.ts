/**
 * Per-district stratified sampling for PLVR transaction features.
 *
 * Two-phase algorithm:
 *   Phase 1 – guarantee `districtMin` most-recent records for every
 *              (city, district) pair present in the corpus.
 *   Phase 2 – fill remaining budget up to `exportLimit` using globally
 *              most-recent transactions not already included in phase 1.
 *
 * Hard cap: when phase 1 alone exceeds `exportLimit` (e.g., many districts
 * each needing `districtMin` records), the combined set is trimmed by global
 * recency to `exportLimit`. This means the per-district guarantee is a
 * best-effort when the budget is very tight; the hard cap always wins.
 *
 * The returned array is sorted newest-first.
 */

export interface SampleItem {
  properties: {
    id: number;
    date: string;
    city: string;
    district: string;
  };
}

export interface StratifiedSampleOptions {
  /** Minimum records to keep per (city, district) pair. Default: 30. */
  districtMin: number;
  /** Hard cap on total output records. Default: 10000. */
  exportLimit: number;
}

/**
 * Return a stratified sample of `allFeatures`.
 * Works on any object whose `.properties` has `{ id, date, city, district }`.
 */
export function stratifiedSample<T extends SampleItem>(
  allFeatures: T[],
  opts: StratifiedSampleOptions,
): T[] {
  // Clamp opts defensively: negative or zero values would produce empty/unexpected results.
  const districtMin = Math.max(1, opts.districtMin);
  const exportLimit = Math.max(0, opts.exportLimit);

  // Group by (city, district). Use \0 as delimiter to avoid collisions for
  // names that themselves contain hyphens (e.g. "A-" + "B" vs "A" + "-B").
  const byDistrict = new Map<string, T[]>();
  for (const f of allFeatures) {
    const key = `${f.properties.city}\0${f.properties.district}`;
    if (!byDistrict.has(key)) byDistrict.set(key, []);
    byDistrict.get(key)!.push(f);
  }

  // Phase 1: per-district minimum (most-recent N per district)
  const guaranteed = new Set<number>();
  const phase1: T[] = [];
  for (const feats of byDistrict.values()) {
    const recent = [...feats].sort(
      (a, b) => b.properties.date.localeCompare(a.properties.date),
    );
    for (const f of recent.slice(0, districtMin)) {
      phase1.push(f);
      guaranteed.add(f.properties.id);
    }
  }

  // Phase 2: fill remaining budget with globally most-recent not already included
  const remaining = allFeatures
    .filter(f => !guaranteed.has(f.properties.id))
    .sort((a, b) => b.properties.date.localeCompare(a.properties.date));

  const combined = [
    ...phase1,
    ...remaining.slice(0, Math.max(0, exportLimit - phase1.length)),
  ];

  // Apply hard cap: if phase1 alone exceeded exportLimit, trim by global recency.
  // Sort the final set newest-first to preserve pre-existing ordering expectations.
  return combined
    .sort((a, b) => b.properties.date.localeCompare(a.properties.date))
    .slice(0, exportLimit);
}

/**
 * Validate per-district coverage depth and emit a warning if too many
 * districts fall below the minimum threshold.
 *
 * @param features     Exported feature set to inspect.
 * @param threshold    Districts with ≤ this many records are "sparse" (default: 10).
 * @param warnFraction Warn if sparse-district fraction exceeds this ratio (default: 0.20).
 */
export function validateDistrictCoverage<T extends SampleItem>(
  features: T[],
  threshold = 10,
  warnFraction = 0.2,
): void {
  const counts = new Map<string, number>();
  for (const f of features) {
    const key = `${f.properties.city}-${f.properties.district}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  const totalDistricts = counts.size;
  const sparseDistricts: string[] = [];
  for (const [key, n] of counts) {
    if (n <= threshold) sparseDistricts.push(`${key}(${n})`);
  }

  const sparseFraction = totalDistricts > 0 ? sparseDistricts.length / totalDistricts : 0;
  const pct = (sparseFraction * 100).toFixed(1);

  console.log(
    `[validate] Per-district coverage: ${totalDistricts} districts total, ` +
    `${sparseDistricts.length} with ≤${threshold} transactions (${pct}%)`,
  );

  if (sparseFraction > warnFraction) {
    console.warn(
      `[validate] WARNING: ${sparseDistricts.length}/${totalDistricts} districts ` +
      `(${pct}%) are below the ${threshold}-transaction threshold — ` +
      `analytics quality may be limited for: ${sparseDistricts.slice(0, 10).join(", ")}` +
      `${sparseDistricts.length > 10 ? " …" : ""}`,
    );
  }
}
