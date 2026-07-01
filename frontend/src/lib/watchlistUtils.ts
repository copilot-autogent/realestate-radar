/**
 * Pure utility functions for the district watchlist feature (issue #110).
 * No DOM or localStorage dependencies — fully testable with Vitest.
 */

export interface WatchlistEntry {
  district: string;
  city: string;
  snapshotPrice: number;  // median unit price in 萬/坪 at time of starring
  snapshotDate: string;   // YYYY-MM-DD
}

export const WL_MAX = 3;
export const WL_STALE_DAYS = 90;

/**
 * Compute the percentage change from snapshotPrice to currentPrice.
 * Returns 0 when snapshotPrice is zero or negative (avoids division by zero).
 */
export function computeDeltaPct(snapshotPrice: number, currentPrice: number): number {
  if (snapshotPrice <= 0) return 0;
  return ((currentPrice - snapshotPrice) / snapshotPrice) * 100;
}

/**
 * Returns true when the snapshot is older than staleAgeDays from now.
 * Uses UTC date-string comparison to avoid timezone off-by-one issues
 * (snapshotDate is always stored as `new Date().toISOString().slice(0,10)`, UTC).
 * Malformed dates (not matching YYYY-MM-DD) are treated as stale.
 */
export function isStale(snapshotDate: string, staleAgeDays = WL_STALE_DAYS): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(snapshotDate)) return true;
  // Compute the cutoff date in UTC without local-time drift
  const cutoff = new Date(Date.now() - staleAgeDays * 24 * 60 * 60 * 1000);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  return snapshotDate < cutoffStr;
}

/**
 * Returns true when the banner should be shown for a watched district.
 * The banner is hidden on the same day the district was starred ("first visit"),
 * and shown on all subsequent visits where the snapshot date is before today.
 *
 * @param snapshotDate - YYYY-MM-DD string of when the district was starred
 * @param todayIso     - YYYY-MM-DD string of today's date
 */
export function shouldShowBanner(snapshotDate: string, todayIso: string): boolean {
  return snapshotDate < todayIso;
}

/**
 * Splits entries into kept (fresh) and cleared (stale ≥ staleAgeDays old).
 */
export function clearStaleEntries(
  entries: WatchlistEntry[],
  staleAgeDays = WL_STALE_DAYS
): { kept: WatchlistEntry[]; cleared: WatchlistEntry[] } {
  const kept: WatchlistEntry[] = [];
  const cleared: WatchlistEntry[] = [];
  for (const e of entries) {
    if (isStale(e.snapshotDate, staleAgeDays)) {
      cleared.push(e);
    } else {
      kept.push(e);
    }
  }
  return { kept, cleared };
}

/**
 * Format a delta percentage for display in the "since your last visit" banner.
 * In real estate context: price increase (↑) is bad for buyers (red),
 * price decrease (↓) is good for buyers (green).
 * The `+` sign is only shown when the arrow is actually ↑ (not on flat).
 *
 * @returns text - formatted string e.g. "+5.3%", "-2.1%", "0.0%"
 *          arrow - directional arrow character
 *          cls   - CSS class name for colour coding
 */
export function formatDelta(deltaPct: number): {
  text: string;
  arrow: string;
  cls: string;
} {
  const rounded = Math.round(deltaPct * 10) / 10;
  const arrow = rounded > 0.5 ? "↑" : rounded < -0.5 ? "↓" : "→";
  const cls =
    rounded > 0.5  ? "wl-delta-up"   :
    rounded < -0.5 ? "wl-delta-down" :
    "wl-delta-flat";
  // Show + only when we actually display the up arrow; flat values show no sign
  const sign = arrow === "↑" ? "+" : "";
  return { text: `${sign}${rounded.toFixed(1)}%`, arrow, cls };
}
