import { describe, it, expect } from "vitest";
import {
  haversineDistanceKm,
  mrtTier,
  polygonCentroid,
  computeMrtProximity,
  MRT_TIER_LABELS,
  MRT_TIER_EMOJIS,
  MRT_TIER_COLORS,
  type MrtStation,
  type MrtTierValue,
} from "../lib/mrtProximityUtils.js";

// ── Fixtures ──────────────────────────────────────────────────────────────────

function makeStation(overrides: Partial<MrtStation> & { name: string }): MrtStation {
  return {
    id: overrides.name,
    line: "R",
    lineZh: "淡水信義線",
    lat: 25.04797,
    lng: 121.51700,
    ...overrides,
  };
}

// Well-known Taipei MRT coordinates (verified against public data)
const ZHONGXIAO_FUXING: MrtStation = makeStation({
  id: "BL14", name: "忠孝復興",
  lat: 25.04162, lng: 121.54450, line: "BL", lineZh: "板南線",
});

const TAIPEI_MAIN: MrtStation = makeStation({
  id: "R11", name: "台北車站",
  lat: 25.04797, lng: 121.51700, line: "R", lineZh: "淡水信義線",
});

const XIANGSHAN: MrtStation = makeStation({
  id: "R04", name: "象山",
  lat: 25.02741, lng: 121.57180, line: "R", lineZh: "淡水信義線",
});

// ── haversineDistanceKm ───────────────────────────────────────────────────────

describe("haversineDistanceKm", () => {
  it("returns 0 for identical points", () => {
    expect(haversineDistanceKm([121.5, 25.0], [121.5, 25.0])).toBe(0);
  });

  it("is commutative", () => {
    const a: [number, number] = [121.54450, 25.04162];
    const b: [number, number] = [121.51700, 25.04797];
    expect(haversineDistanceKm(a, b)).toBeCloseTo(haversineDistanceKm(b, a), 8);
  });

  it("gives ~2.3 km between 忠孝復興 and 台北車站", () => {
    const d = haversineDistanceKm(
      [ZHONGXIAO_FUXING.lng, ZHONGXIAO_FUXING.lat],
      [TAIPEI_MAIN.lng, TAIPEI_MAIN.lat],
    );
    // Haversine straight-line; MRT stations ~2.2–2.5 km apart
    expect(d).toBeGreaterThan(2.0);
    expect(d).toBeLessThan(3.0);
  });

  it("gives <1 km for very close points (within one station spacing)", () => {
    // Move ~300 m east from 台北車站
    const d = haversineDistanceKm([121.517, 25.048], [121.520, 25.048]);
    expect(d).toBeLessThan(0.5);
  });

  it("handles cross-city distances (Taipei to Kaohsiung ~295 km)", () => {
    const d = haversineDistanceKm([121.517, 25.048], [120.304, 22.633]);
    expect(d).toBeGreaterThan(280);
    expect(d).toBeLessThan(320);
  });

  it("handles negative longitude differences correctly", () => {
    const d = haversineDistanceKm([121.60, 25.05], [121.51, 25.05]);
    expect(d).toBeGreaterThan(0);
    expect(d).toBeLessThan(10);
  });

  it("is always non-negative", () => {
    expect(haversineDistanceKm([120.0, 22.0], [121.0, 23.0])).toBeGreaterThan(0);
  });
});

// ── mrtTier ───────────────────────────────────────────────────────────────────

describe("mrtTier", () => {
  it("returns walking for 0 km", () => {
    expect(mrtTier(0)).toBe("walking");
  });

  it("returns walking at exactly 0.5 km", () => {
    expect(mrtTier(0.5)).toBe("walking");
  });

  it("returns nearby just above 0.5 km", () => {
    expect(mrtTier(0.501)).toBe("nearby");
  });

  it("returns nearby at exactly 1.0 km", () => {
    expect(mrtTier(1.0)).toBe("nearby");
  });

  it("returns driving just above 1.0 km", () => {
    expect(mrtTier(1.001)).toBe("driving");
  });

  it("returns driving at exactly 2.0 km", () => {
    expect(mrtTier(2.0)).toBe("driving");
  });

  it("returns remote just above 2.0 km", () => {
    expect(mrtTier(2.001)).toBe("remote");
  });

  it("returns remote for large distances", () => {
    expect(mrtTier(100)).toBe("remote");
    expect(mrtTier(Infinity)).toBe("remote");
  });

  it("covers all four tiers at representative values", () => {
    const tiers: MrtTierValue[] = [
      mrtTier(0.2),   // walking
      mrtTier(0.75),  // nearby
      mrtTier(1.5),   // driving
      mrtTier(3.0),   // remote
    ];
    expect(tiers).toEqual(["walking", "nearby", "driving", "remote"]);
  });
});

// ── polygonCentroid ───────────────────────────────────────────────────────────

describe("polygonCentroid", () => {
  it("returns the single point for a 1-coordinate input", () => {
    expect(polygonCentroid([[121.5, 25.0]])).toEqual([121.5, 25.0]);
  });

  it("returns midpoint for two symmetric points", () => {
    const [lng, lat] = polygonCentroid([[120.0, 24.0], [122.0, 26.0]]);
    expect(lng).toBeCloseTo(121.0);
    expect(lat).toBeCloseTo(25.0);
  });

  it("returns origin for empty array", () => {
    expect(polygonCentroid([])).toEqual([0, 0]);
  });

  it("correctly averages a simple 4-point square", () => {
    const ring: [number, number][] = [
      [121.0, 25.0],
      [122.0, 25.0],
      [122.0, 26.0],
      [121.0, 26.0],
    ];
    const [lng, lat] = polygonCentroid(ring);
    expect(lng).toBeCloseTo(121.5);
    expect(lat).toBeCloseTo(25.5);
  });

  it("handles closed ring (first = last) without special-casing", () => {
    const ring: [number, number][] = [
      [121.0, 25.0],
      [122.0, 25.0],
      [122.0, 26.0],
      [121.0, 26.0],
      [121.0, 25.0], // closed
    ];
    const [lng, lat] = polygonCentroid(ring);
    // Slightly biased toward first/last point, but still in interior
    expect(lng).toBeGreaterThan(121.0);
    expect(lng).toBeLessThan(122.0);
    expect(lat).toBeGreaterThan(25.0);
    expect(lat).toBeLessThan(26.0);
  });
});

// ── computeMrtProximity ───────────────────────────────────────────────────────

describe("computeMrtProximity", () => {
  it("returns noMrt=true and remote tier for empty station list", () => {
    const result = computeMrtProximity([121.5, 25.0], []);
    expect(result.noMrt).toBe(true);
    expect(result.nearestStation).toBeNull();
    expect(result.tier).toBe("remote");
    expect(result.distanceKm).toBe(Infinity);
    expect(result.walkMinutes).toBeNull();
  });

  it("finds the nearest station among multiple candidates", () => {
    const centroid: [number, number] = [ZHONGXIAO_FUXING.lng, ZHONGXIAO_FUXING.lat];
    const result = computeMrtProximity(centroid, [XIANGSHAN, TAIPEI_MAIN, ZHONGXIAO_FUXING]);
    // Centroid is exactly at 忠孝復興 → 0 km away
    expect(result.nearestStation?.id).toBe("BL14");
    expect(result.distanceKm).toBeCloseTo(0, 5);
    expect(result.tier).toBe("walking");
    expect(result.walkMinutes).toBe(0);
    expect(result.noMrt).toBe(false);
  });

  it("assigns walking tier when centroid is 300m from nearest station", () => {
    // Place centroid ~300m east of 台北車站
    const centroid: [number, number] = [121.5208, 25.04797];
    const result = computeMrtProximity(centroid, [TAIPEI_MAIN]);
    expect(result.tier).toBe("walking");
    expect(result.distanceKm).toBeLessThan(0.5);
    expect(result.nearestStation?.name).toBe("台北車站");
  });

  it("assigns nearby tier when centroid is ~700m from nearest station", () => {
    // Place centroid ~700m north of 台北車站
    const centroid: [number, number] = [121.517, 25.054];
    const result = computeMrtProximity(centroid, [TAIPEI_MAIN]);
    expect(result.tier).toBe("nearby");
    expect(result.distanceKm).toBeGreaterThan(0.5);
    expect(result.distanceKm).toBeLessThanOrEqual(1.0);
  });

  it("assigns driving tier when centroid is ~1.5km from nearest station", () => {
    const centroid: [number, number] = [121.530, 25.060];
    const result = computeMrtProximity(centroid, [TAIPEI_MAIN]);
    expect(result.tier).toBe("driving");
    expect(result.distanceKm).toBeGreaterThan(1.0);
    expect(result.distanceKm).toBeLessThanOrEqual(2.0);
  });

  it("assigns remote tier for Kaohsiung district far from any station", () => {
    // Remote hypothetical centroid in rural area, far from all given stations
    const ruralCentroid: [number, number] = [120.70, 22.35];
    const result = computeMrtProximity(ruralCentroid, [TAIPEI_MAIN, ZHONGXIAO_FUXING]);
    expect(result.tier).toBe("remote");
    expect(result.distanceKm).toBeGreaterThan(2.0);
  });

  it("computes walkMinutes as round(distanceKm / 0.08)", () => {
    // Station exactly 0.8 km from centroid → 10 min walk
    const centroid: [number, number] = [121.517, 25.055]; // ~0.8 km north of 台北車站
    const result = computeMrtProximity(centroid, [TAIPEI_MAIN]);
    // walkMinutes should equal Math.round(distanceKm / 0.08)
    const expected = Math.round(result.distanceKm / 0.08);
    expect(result.walkMinutes).toBe(expected);
  });

  it("works with a single-station list", () => {
    const result = computeMrtProximity([121.54450, 25.04162], [ZHONGXIAO_FUXING]);
    expect(result.nearestStation?.name).toBe("忠孝復興");
    expect(result.noMrt).toBe(false);
  });

  it("handles large station arrays efficiently (no crash)", () => {
    const many: MrtStation[] = Array.from({ length: 500 }, (_, i) =>
      makeStation({ name: `Station${i}`, lat: 25.0 + i * 0.001, lng: 121.5 + i * 0.001 }),
    );
    expect(() => computeMrtProximity([121.5, 25.0], many)).not.toThrow();
  });
});

// ── Constants sanity checks ───────────────────────────────────────────────────

describe("MRT_TIER_LABELS", () => {
  it("has entries for all four tiers", () => {
    const tiers: MrtTierValue[] = ["walking", "nearby", "driving", "remote"];
    for (const t of tiers) {
      expect(MRT_TIER_LABELS[t]).toBeTruthy();
    }
  });

  it("uses Chinese labels", () => {
    expect(MRT_TIER_LABELS.walking).toMatch(/步行/);
    expect(MRT_TIER_LABELS.nearby).toMatch(/生活/);
    expect(MRT_TIER_LABELS.driving).toMatch(/搭車/);
    expect(MRT_TIER_LABELS.remote).toMatch(/偏遠/);
  });
});

describe("MRT_TIER_EMOJIS", () => {
  it("has emoji for all four tiers", () => {
    expect(MRT_TIER_EMOJIS.walking).toBe("🟢");
    expect(MRT_TIER_EMOJIS.nearby).toBe("🟡");
    expect(MRT_TIER_EMOJIS.driving).toBe("🟠");
    expect(MRT_TIER_EMOJIS.remote).toBe("🔴");
  });
});

describe("MRT_TIER_COLORS", () => {
  it("provides hex colors for all tiers and noMrt", () => {
    const keys: Array<MrtTierValue | "noMrt"> = [
      "walking", "nearby", "driving", "remote", "noMrt",
    ];
    for (const k of keys) {
      expect(MRT_TIER_COLORS[k]).toMatch(/^#[0-9a-f]{6}$/i);
    }
  });
});
