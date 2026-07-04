import { describe, it, expect } from "vitest";
import {
  SHORTLIST_MAX,
  COMPARISON_MAX,
  parseShortlist,
  addToShortlist,
  removeFromShortlist,
  clearShortlist,
  pruneStaleIds,
  buildComparisonRows,
  type ShortlistEntry,
  type DistrictStatsForComparison,
} from "../lib/shortlistUtils.js";

// ── Fixtures ──────────────────────────────────────────────────────────────────

function makeEntry(districtId: string, addedAt?: string): ShortlistEntry {
  const [city, district] = districtId.split(":");
  return {
    districtId,
    city: city ?? "",
    label: district ?? districtId,
    addedAt: addedAt ?? "2024-01-01T00:00:00.000Z",
  };
}

function makeStats(
  districtId: string,
  overrides: Partial<DistrictStatsForComparison> = {}
): DistrictStatsForComparison {
  return {
    districtId,
    medianPriceWan: 60,
    buyerTimingScore: 70,
    negotiationMarginPct: 0.07,
    medianBuildingAge: 15,
    firstBuyerAffordabilityRatio: 80,
    nearestMrt: "信義安和",
    seasonalIndex: 65,
    ...overrides,
  };
}

// ── Constants ─────────────────────────────────────────────────────────────────

describe("constants", () => {
  it("SHORTLIST_MAX is 5", () => {
    expect(SHORTLIST_MAX).toBe(5);
  });

  it("COMPARISON_MAX is 3", () => {
    expect(COMPARISON_MAX).toBe(3);
  });
});

// ── parseShortlist ────────────────────────────────────────────────────────────

describe("parseShortlist", () => {
  it("returns empty array for null", () => {
    expect(parseShortlist(null)).toEqual([]);
  });

  it("returns empty array for empty string", () => {
    expect(parseShortlist("")).toEqual([]);
  });

  it("returns empty array for invalid JSON", () => {
    expect(parseShortlist("{not-json")).toEqual([]);
  });

  it("returns empty array for non-array JSON", () => {
    expect(parseShortlist('{"a":1}')).toEqual([]);
  });

  it("parses a valid array of entries", () => {
    const entry = makeEntry("台北市:大安區");
    expect(parseShortlist(JSON.stringify([entry]))).toEqual([entry]);
  });

  it("filters out entries missing required fields", () => {
    const valid = makeEntry("台北市:大安區");
    const invalid = { districtId: "", city: "台北市", label: "大安區", addedAt: "2024-01-01T00:00:00.000Z" };
    const result = parseShortlist(JSON.stringify([valid, invalid]));
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual(valid);
  });

  it("filters out non-object entries", () => {
    const valid = makeEntry("台北市:信義區");
    expect(parseShortlist(JSON.stringify([valid, null, 42, "str"]))).toEqual([valid]);
  });
});

// ── addToShortlist ────────────────────────────────────────────────────────────

describe("addToShortlist", () => {
  it("adds a new entry to an empty list", () => {
    const e = makeEntry("台北市:大安區");
    const result = addToShortlist([], e);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual(e);
  });

  it("appends to an existing list", () => {
    const e1 = makeEntry("台北市:大安區");
    const e2 = makeEntry("台北市:信義區");
    const result = addToShortlist([e1], e2);
    expect(result).toHaveLength(2);
    expect(result[1]).toEqual(e2);
  });

  it("replaces an existing entry with the same districtId (updates addedAt)", () => {
    const old = makeEntry("台北市:大安區", "2024-01-01T00:00:00.000Z");
    const updated = makeEntry("台北市:大安區", "2025-06-01T00:00:00.000Z");
    const result = addToShortlist([old], updated);
    expect(result).toHaveLength(1);
    expect(result[0].addedAt).toBe("2025-06-01T00:00:00.000Z");
  });

  it("does not exceed SHORTLIST_MAX (5) — evicts oldest on add", () => {
    const entries: ShortlistEntry[] = [];
    for (let i = 0; i < SHORTLIST_MAX; i++) {
      entries.push(makeEntry(`台北市:區${i}`, `2024-0${i + 1}-01T00:00:00.000Z`));
    }
    const newest = makeEntry("台北市:新區", "2024-12-01T00:00:00.000Z");
    const result = addToShortlist(entries, newest);
    expect(result).toHaveLength(SHORTLIST_MAX);
    // Oldest entry (區0, 2024-01) should be evicted
    expect(result.some((e) => e.districtId === "台北市:區0")).toBe(false);
    // Newest should be present
    expect(result.some((e) => e.districtId === "台北市:新區")).toBe(true);
  });

  it("evicts the single oldest when list is exactly at cap", () => {
    const entries: ShortlistEntry[] = [
      makeEntry("台北市:區A", "2024-01-01T00:00:00.000Z"),
      makeEntry("台北市:區B", "2024-02-01T00:00:00.000Z"),
      makeEntry("台北市:區C", "2024-03-01T00:00:00.000Z"),
      makeEntry("台北市:區D", "2024-04-01T00:00:00.000Z"),
      makeEntry("台北市:區E", "2024-05-01T00:00:00.000Z"),
    ];
    const result = addToShortlist(entries, makeEntry("台北市:區F", "2024-06-01T00:00:00.000Z"));
    expect(result).toHaveLength(SHORTLIST_MAX);
    expect(result.some((e) => e.districtId === "台北市:區A")).toBe(false);
    expect(result.some((e) => e.districtId === "台北市:區F")).toBe(true);
  });

  it("does not modify the original array (immutable)", () => {
    const original = [makeEntry("台北市:大安區")];
    addToShortlist(original, makeEntry("台北市:信義區"));
    expect(original).toHaveLength(1);
  });
});

// ── removeFromShortlist ───────────────────────────────────────────────────────

describe("removeFromShortlist", () => {
  it("removes an entry by districtId", () => {
    const entries = [makeEntry("台北市:大安區"), makeEntry("台北市:信義區")];
    const result = removeFromShortlist(entries, "台北市:大安區");
    expect(result).toHaveLength(1);
    expect(result[0].districtId).toBe("台北市:信義區");
  });

  it("is a no-op when districtId not found", () => {
    const entries = [makeEntry("台北市:大安區")];
    const result = removeFromShortlist(entries, "台北市:不存在");
    expect(result).toHaveLength(1);
  });

  it("returns empty array when removing the last entry", () => {
    const entries = [makeEntry("台北市:大安區")];
    expect(removeFromShortlist(entries, "台北市:大安區")).toEqual([]);
  });

  it("does not modify the original array (immutable)", () => {
    const original = [makeEntry("台北市:大安區"), makeEntry("台北市:信義區")];
    removeFromShortlist(original, "台北市:大安區");
    expect(original).toHaveLength(2);
  });
});

// ── clearShortlist ────────────────────────────────────────────────────────────

describe("clearShortlist", () => {
  it("returns an empty array", () => {
    expect(clearShortlist()).toEqual([]);
  });

  it("always returns an empty array regardless of prior state", () => {
    const result = clearShortlist();
    expect(Array.isArray(result)).toBe(true);
    expect(result).toHaveLength(0);
  });
});

// ── pruneStaleIds ─────────────────────────────────────────────────────────────

describe("pruneStaleIds", () => {
  it("keeps entries whose districtId is in the valid set", () => {
    const entries = [makeEntry("台北市:大安區"), makeEntry("台北市:信義區")];
    const { kept, pruned } = pruneStaleIds(entries, new Set(["台北市:大安區", "台北市:信義區"]));
    expect(kept).toHaveLength(2);
    expect(pruned).toHaveLength(0);
  });

  it("prunes entries whose districtId is NOT in the valid set", () => {
    const entries = [makeEntry("台北市:大安區"), makeEntry("台北市:消失區")];
    const { kept, pruned } = pruneStaleIds(entries, new Set(["台北市:大安區"]));
    expect(kept).toHaveLength(1);
    expect(kept[0].districtId).toBe("台北市:大安區");
    expect(pruned).toHaveLength(1);
    expect(pruned[0].districtId).toBe("台北市:消失區");
  });

  it("prunes all entries when valid set is empty", () => {
    const entries = [makeEntry("台北市:大安區"), makeEntry("台北市:信義區")];
    const { kept, pruned } = pruneStaleIds(entries, new Set());
    expect(kept).toHaveLength(0);
    expect(pruned).toHaveLength(2);
  });

  it("returns empty arrays for empty entry list", () => {
    const { kept, pruned } = pruneStaleIds([], new Set(["台北市:大安區"]));
    expect(kept).toHaveLength(0);
    expect(pruned).toHaveLength(0);
  });
});

// ── buildComparisonRows ───────────────────────────────────────────────────────

describe("buildComparisonRows", () => {
  const e1 = makeEntry("台北市:大安區");
  const e2 = makeEntry("台北市:信義區");
  const e3 = makeEntry("台北市:中山區");

  it("returns empty array when entries list is empty", () => {
    expect(buildComparisonRows([], new Map())).toEqual([]);
  });

  it("returns empty array when no entries have stats", () => {
    expect(buildComparisonRows([e1], new Map())).toEqual([]);
  });

  it("produces 7 rows for a 2-district comparison", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區")],
      ["台北市:信義區", makeStats("台北市:信義區")],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    expect(rows).toHaveLength(7);
  });

  it("row keys include all expected metric keys", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區")],
      ["台北市:信義區", makeStats("台北市:信義區")],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const keys = rows.map((r) => r.key);
    expect(keys).toContain("medianPriceWan");
    expect(keys).toContain("buyerTimingScore");
    expect(keys).toContain("negotiationMarginPct");
    expect(keys).toContain("medianBuildingAge");
    expect(keys).toContain("firstBuyerAffordabilityRatio");
    expect(keys).toContain("nearestMrt");
    expect(keys).toContain("seasonalIndex");
  });

  it("respects COMPARISON_MAX — only first 3 entries are included", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區")],
      ["台北市:信義區", makeStats("台北市:信義區")],
      ["台北市:中山區", makeStats("台北市:中山區")],
      ["台北市:松山區", makeStats("台北市:松山區")],
    ]);
    const entries = [e1, e2, e3, makeEntry("台北市:松山區")];
    const rows = buildComparisonRows(entries, statsMap);
    // Values should only have keys for first 3 entries
    const ids = Object.keys(rows[0]!.values);
    expect(ids).toHaveLength(3);
    expect(ids).not.toContain("台北市:松山區");
  });

  it("medianPriceWan row: lower district gets bestDistrictId", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { medianPriceWan: 80 })],
      ["台北市:信義區", makeStats("台北市:信義區", { medianPriceWan: 60 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const priceRow = rows.find((r) => r.key === "medianPriceWan")!;
    expect(priceRow.bestDistrictId).toBe("台北市:信義區");
  });

  it("buyerTimingScore row: higher district gets bestDistrictId", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { buyerTimingScore: 55 })],
      ["台北市:信義區", makeStats("台北市:信義區", { buyerTimingScore: 85 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "buyerTimingScore")!;
    expect(row.bestDistrictId).toBe("台北市:信義區");
  });

  it("medianBuildingAge row: lower age district gets bestDistrictId", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { medianBuildingAge: 30 })],
      ["台北市:信義區", makeStats("台北市:信義區", { medianBuildingAge: 10 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "medianBuildingAge")!;
    expect(row.bestDistrictId).toBe("台北市:信義區");
  });

  it("nearestMrt row: bestDistrictId is always null (non-numeric)", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { nearestMrt: "大安" })],
      ["台北市:信義區", makeStats("台北市:信義區", { nearestMrt: "象山" })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "nearestMrt")!;
    expect(row.bestDistrictId).toBeNull();
  });

  it("bestDistrictId is null when only one district has valid data (no best to pick)", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { buyerTimingScore: 70 })],
      ["台北市:信義區", makeStats("台北市:信義區", { buyerTimingScore: null })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "buyerTimingScore")!;
    expect(row.bestDistrictId).toBeNull();
  });

  it("formats medianPriceWan values with ' 萬' suffix", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { medianPriceWan: 75.5 })],
      ["台北市:信義區", makeStats("台北市:信義區", { medianPriceWan: 60.0 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "medianPriceWan")!;
    expect(row.values["台北市:大安區"]).toBe("75.5 萬");
    expect(row.values["台北市:信義區"]).toBe("60.0 萬");
  });

  it("formats negotiationMarginPct as percentage string", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { negotiationMarginPct: 0.072 })],
      ["台北市:信義區", makeStats("台北市:信義區", { negotiationMarginPct: 0.1 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "negotiationMarginPct")!;
    expect(row.values["台北市:大安區"]).toBe("7.2%");
    expect(row.values["台北市:信義區"]).toBe("10.0%");
  });

  it("shows '—' for null values", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { medianPriceWan: null })],
      ["台北市:信義區", makeStats("台北市:信義區", { medianPriceWan: 60 })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "medianPriceWan")!;
    expect(row.values["台北市:大安區"]).toBe("—");
  });

  it("shows nearestMrt name directly (or '—' when null)", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區", { nearestMrt: "大安" })],
      ["台北市:信義區", makeStats("台北市:信義區", { nearestMrt: null })],
    ]);
    const rows = buildComparisonRows([e1, e2], statsMap);
    const row = rows.find((r) => r.key === "nearestMrt")!;
    expect(row.values["台北市:大安區"]).toBe("大安");
    expect(row.values["台北市:信義區"]).toBe("—");
  });

  it("handles a single entry (no best-value highlighting for numeric rows)", () => {
    const statsMap = new Map([
      ["台北市:大安區", makeStats("台北市:大安區")],
    ]);
    const rows = buildComparisonRows([e1], statsMap);
    expect(rows).toHaveLength(7);
    // With only 1 entry, pickBest returns null (needs ≥2 valid candidates)
    rows.forEach((r) => expect(r.bestDistrictId).toBeNull());
  });
});
