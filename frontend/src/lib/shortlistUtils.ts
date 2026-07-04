/**
 * Pure utility functions for the district comparison shortlist feature (issue #145).
 * No DOM or localStorage dependencies — fully testable with Vitest.
 *
 * localStorage key: `rr_shortlist_v1`
 */

export const SHORTLIST_KEY = "rr_shortlist_v1";
export const SHORTLIST_MAX = 5;
export const COMPARISON_MAX = 3;

// ── Types ─────────────────────────────────────────────────────────────────────

export interface ShortlistEntry {
  /** Composite key: "{city}:{district}" */
  districtId: string;
  city: string;
  label: string;
  addedAt: string; // ISO 8601 date-time string
}

/**
 * Per-district stats passed to buildComparisonRows.
 * All optional so callers can provide partial data when some layers are absent.
 */
export interface DistrictStatsForComparison {
  districtId: string;
  /** Median unit price in 萬/坪 (12-month window). Null = insufficient data. */
  medianPriceWan: number | null;
  /** Buyer timing score 0–100. Null = insufficient data. */
  buyerTimingScore: number | null;
  /** Negotiation margin centre estimate (0–1 scale, e.g. 0.07 = 7%). Null = insufficient. */
  negotiationMarginPct: number | null;
  /** Median building age in years. Null = insufficient data. */
  medianBuildingAge: number | null;
  /** First-buyer affordability ratio 0–100+ (100 = fully affordable). Null = not computed. */
  firstBuyerAffordabilityRatio: number | null;
  /** Nearest MRT station name. Null = MRT data unavailable or not computed. */
  nearestMrt: string | null;
  /** Seasonal index: 0–100 score for current month (higher = better buying month). Null = unavailable. */
  seasonalIndex: number | null;
}

/** A single row of the comparison table. */
export interface ComparisonRow {
  /** Row label shown in the table header column. */
  label: string;
  /** Row key for programmatic access. */
  key: keyof DistrictStatsForComparison | "district";
  /**
   * Per-district display values.
   * Keys are districtIds; values are the formatted string to show (or "—" when null).
   */
  values: Record<string, string>;
  /**
   * districtId with the best value in this row, or null when no row has sufficient data.
   * Used to apply a green highlight on the best cell.
   */
  bestDistrictId: string | null;
}

// ── localStorage helpers (pure — accept/return plain data, no DOM) ────────────

/**
 * Parse and validate a shortlist from a raw JSON string (e.g., localStorage value).
 * Returns an empty array on parse failure or invalid shape.
 */
export function parseShortlist(raw: string | null): ShortlistEntry[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (e): e is ShortlistEntry =>
        e !== null &&
        typeof e === "object" &&
        typeof e.districtId === "string" && e.districtId.length > 0 &&
        typeof e.city === "string" &&
        typeof e.label === "string" &&
        typeof e.addedAt === "string"
    );
  } catch {
    return [];
  }
}

/**
 * Add an entry to the shortlist.
 *
 * Rules:
 * - If the districtId already exists, update addedAt (move to top / refresh).
 * - If the list is full (≥ SHORTLIST_MAX), remove the oldest entry first.
 * - Returns the new list (caller persists via saveShortlist).
 */
export function addToShortlist(
  entries: ShortlistEntry[],
  entry: ShortlistEntry
): ShortlistEntry[] {
  // Remove any existing entry with the same districtId
  const without = entries.filter((e) => e.districtId !== entry.districtId);

  // Evict oldest when at capacity
  let base = without;
  if (base.length >= SHORTLIST_MAX) {
    // Sort ascending by addedAt, remove the oldest
    const sorted = [...base].sort((a, b) => a.addedAt.localeCompare(b.addedAt));
    base = sorted.slice(1);
  }

  return [...base, entry];
}

/**
 * Remove an entry by districtId.
 * Returns the new list (caller persists).
 */
export function removeFromShortlist(
  entries: ShortlistEntry[],
  districtId: string
): ShortlistEntry[] {
  return entries.filter((e) => e.districtId !== districtId);
}

/** Returns an empty array. Caller persists via saveShortlist. */
export function clearShortlist(): ShortlistEntry[] {
  return [];
}

/**
 * Prune entries whose districtId is no longer present in the set of valid IDs.
 * Returns { kept, pruned }.
 */
export function pruneStaleIds(
  entries: ShortlistEntry[],
  validDistrictIds: Set<string>
): { kept: ShortlistEntry[]; pruned: ShortlistEntry[] } {
  const kept: ShortlistEntry[] = [];
  const pruned: ShortlistEntry[] = [];
  for (const e of entries) {
    if (validDistrictIds.has(e.districtId)) {
      kept.push(e);
    } else {
      pruned.push(e);
    }
  }
  return { kept, pruned };
}

// ── Comparison table logic ────────────────────────────────────────────────────

/** Format a nullable number as a string with a fixed number of decimal places, or "—". */
function fmt(value: number | null, decimals = 1, suffix = ""): string {
  if (value === null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(decimals)}${suffix}`;
}

/**
 * Identify which districtId has the best (highest) value among a set.
 * Returns null when fewer than 2 entries have valid (non-null) data.
 */
function pickBest(
  districtIds: string[],
  getValue: (id: string) => number | null,
  /** True = lower is better (e.g. building age) */
  lowerIsBetter = false
): string | null {
  const candidates = districtIds
    .map((id) => ({ id, v: getValue(id) }))
    .filter((c): c is { id: string; v: number } => c.v !== null && Number.isFinite(c.v));

  if (candidates.length < 2) return null;

  const best = candidates.reduce((a, b) => {
    if (lowerIsBetter) return a.v <= b.v ? a : b;
    return a.v >= b.v ? a : b;
  });
  return best.id;
}

/**
 * Build comparison table rows from a list of shortlist entries and their stats.
 *
 * Rows follow the spec order:
 *   1. 中位坪單價 (lower = better)
 *   2. 買方時機分 (higher = better)
 *   3. 議價空間% (higher = better)
 *   4. 屋齡中位數 (lower = better)
 *   5. 首購族可負擔性 (higher = better)
 *   6. 最近捷運 (informational only, no best-value highlight)
 *   7. 季節指數 (higher = better)
 *
 * Only entries with districtId present in statsMap are included.
 * At most COMPARISON_MAX entries are used (caller should pre-slice).
 */
export function buildComparisonRows(
  entries: ShortlistEntry[],
  statsMap: Map<string, DistrictStatsForComparison>
): ComparisonRow[] {
  // Only include entries that have stats
  const active = entries
    .filter((e) => statsMap.has(e.districtId))
    .slice(0, COMPARISON_MAX);

  if (active.length === 0) return [];

  const ids = active.map((e) => e.districtId);
  const getStats = (id: string): DistrictStatsForComparison =>
    statsMap.get(id) ?? {
      districtId: id,
      medianPriceWan: null,
      buyerTimingScore: null,
      negotiationMarginPct: null,
      medianBuildingAge: null,
      firstBuyerAffordabilityRatio: null,
      nearestMrt: null,
      seasonalIndex: null,
    };

  const rows: ComparisonRow[] = [
    {
      label: "中位坪單價（萬/坪）",
      key: "medianPriceWan",
      values: Object.fromEntries(
        ids.map((id) => [id, fmt(getStats(id).medianPriceWan, 1, " 萬")])
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).medianPriceWan, true),
    },
    {
      label: "買方時機分",
      key: "buyerTimingScore",
      values: Object.fromEntries(
        ids.map((id) => {
          const s = getStats(id).buyerTimingScore;
          return [id, s !== null && Number.isFinite(s) ? `${s} 分` : "—"];
        })
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).buyerTimingScore),
    },
    {
      label: "議價空間",
      key: "negotiationMarginPct",
      values: Object.fromEntries(
        ids.map((id) => {
          const v = getStats(id).negotiationMarginPct;
          if (v === null || !Number.isFinite(v)) return [id, "—"];
          return [id, `${(v * 100).toFixed(1)}%`];
        })
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).negotiationMarginPct),
    },
    {
      label: "屋齡中位數",
      key: "medianBuildingAge",
      values: Object.fromEntries(
        ids.map((id) => {
          const v = getStats(id).medianBuildingAge;
          return [id, v !== null && Number.isFinite(v) ? `${v} 年` : "—"];
        })
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).medianBuildingAge, true),
    },
    {
      label: "首購族可負擔性",
      key: "firstBuyerAffordabilityRatio",
      values: Object.fromEntries(
        ids.map((id) => {
          const v = getStats(id).firstBuyerAffordabilityRatio;
          if (v === null || !Number.isFinite(v)) return [id, "—"];
          return [id, `${Math.round(v)}%`];
        })
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).firstBuyerAffordabilityRatio),
    },
    {
      label: "最近捷運",
      key: "nearestMrt",
      values: Object.fromEntries(
        ids.map((id) => [id, getStats(id).nearestMrt ?? "—"])
      ),
      // Nearest MRT is a name, not a numeric score — no best-value highlight
      bestDistrictId: null,
    },
    {
      label: "季節指數",
      key: "seasonalIndex",
      values: Object.fromEntries(
        ids.map((id) => {
          const v = getStats(id).seasonalIndex;
          if (v === null || !Number.isFinite(v)) return [id, "—"];
          return [id, `${Math.round(v)} 分`];
        })
      ),
      bestDistrictId: pickBest(ids, (id) => getStats(id).seasonalIndex),
    },
  ];

  return rows;
}
