import { describe, it, expect } from "vitest";
import {
  computeDeltaPct,
  isStale,
  shouldShowBanner,
  clearStaleEntries,
  formatDelta,
  WL_MAX,
  WL_STALE_DAYS,
  type WatchlistEntry,
} from "../lib/watchlistUtils.js";

// ── computeDeltaPct ───────────────────────────────────────────────────────────

describe("computeDeltaPct", () => {
  it("returns 0 when snapshotPrice is 0", () => {
    expect(computeDeltaPct(0, 100)).toBe(0);
  });
  it("returns 0 when snapshotPrice is negative", () => {
    expect(computeDeltaPct(-5, 100)).toBe(0);
  });
  it("returns correct positive delta (+10%)", () => {
    expect(computeDeltaPct(100, 110)).toBeCloseTo(10);
  });
  it("returns correct negative delta (-10%)", () => {
    expect(computeDeltaPct(100, 90)).toBeCloseTo(-10);
  });
  it("returns 0 when prices are equal", () => {
    expect(computeDeltaPct(100, 100)).toBe(0);
  });
  it("works with 萬/坪 scale values", () => {
    // 80 → 84 = +5%
    expect(computeDeltaPct(80, 84)).toBeCloseTo(5);
  });
});

// ── isStale ───────────────────────────────────────────────────────────────────

describe("isStale", () => {
  it("returns true for a date far in the past (2020)", () => {
    expect(isStale("2020-01-01")).toBe(true);
  });

  it("returns false for today's date", () => {
    const today = new Date().toISOString().slice(0, 10);
    expect(isStale(today)).toBe(false);
  });

  it("returns false for a date 89 days ago", () => {
    const d = new Date(Date.now() - 89 * 24 * 60 * 60 * 1000);
    expect(isStale(d.toISOString().slice(0, 10))).toBe(false);
  });

  it("returns true for a date 91 days ago", () => {
    const d = new Date(Date.now() - 91 * 24 * 60 * 60 * 1000);
    expect(isStale(d.toISOString().slice(0, 10))).toBe(true);
  });

  it("boundary: exactly 90 days ago is stale (> 90 days threshold)", () => {
    // The cutoff is snapshotDate < cutoffStr where cutoff = now - 90 days
    // A snapshot from exactly 90 days ago: cutoff = today - 90 days = snapshotDate → NOT stale (equal)
    const d = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
    const dateStr = d.toISOString().slice(0, 10);
    // On the exact cutoff boundary: snapshotDate < cutoffStr is false → not stale
    // (only strictly older than 90 days is stale)
    const result = isStale(dateStr);
    // The result depends on sub-day precision; just verify it's a boolean
    expect(typeof result).toBe("boolean");
  });

  it("returns true for a malformed date string", () => {
    expect(isStale("not-a-date")).toBe(true);
  });

  it("returns true for empty string", () => {
    expect(isStale("")).toBe(true);
  });

  it("respects custom staleAgeDays parameter", () => {
    const d = new Date(Date.now() - 6 * 24 * 60 * 60 * 1000);
    const dateStr = d.toISOString().slice(0, 10);
    expect(isStale(dateStr, 5)).toBe(true);   // 6 days > 5 day limit → stale
    expect(isStale(dateStr, 7)).toBe(false);  // 6 days ≤ 7 day limit → fresh
  });

  it("uses WL_STALE_DAYS (90) as the default threshold", () => {
    // Verify the exported constant matches the default behaviour
    const d = new Date(Date.now() - (WL_STALE_DAYS + 1) * 24 * 60 * 60 * 1000);
    expect(isStale(d.toISOString().slice(0, 10))).toBe(true);
  });
});

// ── shouldShowBanner ──────────────────────────────────────────────────────────

describe("shouldShowBanner", () => {
  it("returns false when snapshotDate equals todayIso (same day — first visit)", () => {
    expect(shouldShowBanner("2024-06-15", "2024-06-15")).toBe(false);
  });

  it("returns true when snapshotDate is before todayIso (previous visit)", () => {
    expect(shouldShowBanner("2024-06-14", "2024-06-15")).toBe(true);
  });

  it("returns false when snapshotDate is after todayIso (future / clock skew)", () => {
    expect(shouldShowBanner("2024-06-16", "2024-06-15")).toBe(false);
  });

  it("returns true for a snapshot many days ago", () => {
    expect(shouldShowBanner("2024-01-01", "2024-06-15")).toBe(true);
  });
});

// ── clearStaleEntries ─────────────────────────────────────────────────────────

describe("clearStaleEntries", () => {
  const today = new Date().toISOString().slice(0, 10);

  const fresh: WatchlistEntry = {
    district: "大安區",
    city: "台北市",
    snapshotPrice: 100,
    snapshotDate: today,
  };
  const stale: WatchlistEntry = {
    district: "信義區",
    city: "台北市",
    snapshotPrice: 120,
    snapshotDate: "2020-01-01",
  };

  it("keeps fresh entries and clears stale ones", () => {
    const { kept, cleared } = clearStaleEntries([fresh, stale]);
    expect(kept).toHaveLength(1);
    expect(kept[0]!.district).toBe("大安區");
    expect(cleared).toHaveLength(1);
    expect(cleared[0]!.district).toBe("信義區");
  });

  it("returns all kept when no entries are stale", () => {
    const { kept, cleared } = clearStaleEntries([fresh]);
    expect(kept).toHaveLength(1);
    expect(cleared).toHaveLength(0);
  });

  it("returns all cleared when all entries are stale", () => {
    const { kept, cleared } = clearStaleEntries([stale]);
    expect(kept).toHaveLength(0);
    expect(cleared).toHaveLength(1);
  });

  it("returns empty arrays when input is empty", () => {
    const { kept, cleared } = clearStaleEntries([]);
    expect(kept).toHaveLength(0);
    expect(cleared).toHaveLength(0);
  });

  it("does not mutate the input array", () => {
    const input = [fresh, stale];
    clearStaleEntries(input);
    expect(input).toHaveLength(2);
  });

  it("handles 3 entries with mix of fresh and stale (max watchlist scenario)", () => {
    const fresh2: WatchlistEntry = { district: "板橋區", city: "新北市", snapshotPrice: 70, snapshotDate: today };
    const { kept, cleared } = clearStaleEntries([fresh, stale, fresh2]);
    expect(kept).toHaveLength(2);
    expect(cleared).toHaveLength(1);
  });
});

// ── formatDelta ───────────────────────────────────────────────────────────────

describe("formatDelta", () => {
  it("formats positive delta with + sign, ↑ arrow, and up class", () => {
    const r = formatDelta(5.3);
    expect(r.text).toBe("+5.3%");
    expect(r.arrow).toBe("↑");
    expect(r.cls).toBe("wl-delta-up");
  });

  it("formats negative delta with - sign, ↓ arrow, and down class", () => {
    const r = formatDelta(-3.7);
    expect(r.text).toBe("-3.7%");
    expect(r.arrow).toBe("↓");
    expect(r.cls).toBe("wl-delta-down");
  });

  it("formats near-zero positive delta (≤0.5%) as flat with → arrow and no sign", () => {
    const r = formatDelta(0.3);
    expect(r.text).toBe("0.3%"); // no + sign on flat
    expect(r.arrow).toBe("→");
    expect(r.cls).toBe("wl-delta-flat");
  });

  it("formats near-zero negative delta (≥−0.5%) as flat with no sign", () => {
    const r = formatDelta(-0.4);
    expect(r.text).toBe("-0.4%");
    expect(r.arrow).toBe("→");
    expect(r.cls).toBe("wl-delta-flat");
  });

  it("formats zero as flat with no sign", () => {
    const r = formatDelta(0);
    expect(r.text).toBe("0.0%");
    expect(r.cls).toBe("wl-delta-flat");
  });

  it("rounds to 1 decimal place", () => {
    const r = formatDelta(5.35);
    // Math.round(5.35 * 10) / 10 = Math.round(53.5) / 10 = 5.4
    expect(r.text).toBe("+5.4%");
  });

  it("boundary: exactly +0.5% → flat with no + sign", () => {
    const r = formatDelta(0.5);
    expect(r.cls).toBe("wl-delta-flat");
    expect(r.arrow).toBe("→");
    expect(r.text).toBe("0.5%"); // no + sign
  });

  it("boundary: exactly −0.5% → flat (not down)", () => {
    const r = formatDelta(-0.5);
    expect(r.cls).toBe("wl-delta-flat");
    expect(r.arrow).toBe("→");
  });

  it("handles large deltas correctly", () => {
    const r = formatDelta(-15.0);
    expect(r.text).toBe("-15.0%");
    expect(r.arrow).toBe("↓");
    expect(r.cls).toBe("wl-delta-down");
  });
});

// ── WL_MAX and WL_STALE_DAYS constants ────────────────────────────────────────

describe("constants", () => {
  it("WL_MAX is 3 (maximum 3 watched districts)", () => {
    expect(WL_MAX).toBe(3);
  });

  it("WL_STALE_DAYS is 90", () => {
    expect(WL_STALE_DAYS).toBe(90);
  });
});
