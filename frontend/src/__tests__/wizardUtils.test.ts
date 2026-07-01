import { describe, it, expect } from "vitest";
import {
  haversineKm,
  estimateCommuteMins,
  maxDistanceForMins,
  polygonCentroid,
  filterByBudget,
  filterByCommute,
  sortByPriority,
  runWizard,
  COMMUTE_HUBS,
  type DistrictInput,
  type WizardParams,
} from "../lib/wizardUtils.js";

// ── Test data factories ────────────────────────────────────────────────────────

function makeDistrict(
  district: string,
  city: string,
  medianPriceWan: number | null,
  buyerTimingScore: number | null,
  lat = 25.04,
  lng = 121.52,
  txCount12mo = 20
): DistrictInput {
  return { district, city, medianPriceWan, buyerTimingScore, lat, lng, txCount12mo, yoyPct: null };
}

// Districts near 台北車站 (within 15 km)
const nearTaipei = makeDistrict("大安區", "台北市", 80, 60, 25.026, 121.543);
const midTaipei  = makeDistrict("松山區", "台北市", 50, 70, 25.05,  121.577);
const farDistrict = makeDistrict("淡水區", "新北市", 20, 80, 25.17,  121.44);   // ~15+ km
const veryFar    = makeDistrict("新竹東區", "新竹市", 15, 85, 24.803, 120.972); // ~80 km

// ── haversineKm ───────────────────────────────────────────────────────────────

describe("haversineKm", () => {
  it("returns 0 for identical coordinates", () => {
    expect(haversineKm(25.0, 121.5, 25.0, 121.5)).toBe(0);
  });

  it("computes reasonable straight-line distance between Taipei and Taichung (~130 km)", () => {
    const km = haversineKm(25.05, 121.52, 24.14, 120.68);
    expect(km).toBeGreaterThan(120);
    expect(km).toBeLessThan(160);
  });

  it("is symmetric", () => {
    const ab = haversineKm(25.05, 121.52, 24.14, 120.68);
    const ba = haversineKm(24.14, 120.68, 25.05, 121.52);
    expect(Math.abs(ab - ba)).toBeLessThan(0.001);
  });
});

// ── estimateCommuteMins / maxDistanceForMins ──────────────────────────────────

describe("estimateCommuteMins", () => {
  it("15 km → 30 minutes", () => {
    expect(estimateCommuteMins(15)).toBeCloseTo(30, 1);
  });
  it("0 km → 0 minutes", () => {
    expect(estimateCommuteMins(0)).toBe(0);
  });
});

describe("maxDistanceForMins", () => {
  it("30 min → 15 km", () => {
    expect(maxDistanceForMins(30)).toBeCloseTo(15, 1);
  });
  it("60 min → 30 km", () => {
    expect(maxDistanceForMins(60)).toBeCloseTo(30, 1);
  });
  it("maxDistance and estimateCommuteMins are inverses", () => {
    const km = maxDistanceForMins(45);
    expect(estimateCommuteMins(km)).toBeCloseTo(45, 1);
  });
});

// ── polygonCentroid ───────────────────────────────────────────────────────────

describe("polygonCentroid", () => {
  it("returns null for null geometry", () => {
    expect(polygonCentroid(null as any)).toBeNull();
  });

  it("returns null for unknown geometry type", () => {
    expect(polygonCentroid({ type: "Point", coordinates: [121, 25] })).toBeNull();
  });

  it("computes centroid of a square Polygon", () => {
    // Square from [0,0] to [2,2] — centroid should be [1,1]
    const geom = {
      type: "Polygon",
      coordinates: [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
    };
    const result = polygonCentroid(geom);
    expect(result).not.toBeNull();
    expect(result!.lng).toBeCloseTo(1, 5);
    expect(result!.lat).toBeCloseTo(1, 5);
  });

  it("computes centroid of a MultiPolygon (uses outer ring of first polygon)", () => {
    const geom = {
      type: "MultiPolygon",
      coordinates: [[[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]],
    };
    const result = polygonCentroid(geom);
    expect(result).not.toBeNull();
    expect(result!.lng).toBeCloseTo(2, 5);
    expect(result!.lat).toBeCloseTo(2, 5);
  });
});

// ── filterByBudget ────────────────────────────────────────────────────────────

describe("filterByBudget", () => {
  const districts: DistrictInput[] = [
    makeDistrict("A", "台北市", 15, 50),   // 15 萬/坪 → < 800萬
    makeDistrict("B", "台北市", 35, 55),   // 35 → 800–1500
    makeDistrict("C", "台北市", 70, 60),   // 70 → 1500–2500
    makeDistrict("D", "台北市", 100, 65),  // 100 → > 2500
    makeDistrict("E", "台北市", null, 50), // null → always excluded
  ];

  it("under-800 includes only cheapest district", () => {
    const result = filterByBudget(districts, "under-800");
    expect(result.map((d) => d.district)).toContain("A");
    expect(result.map((d) => d.district)).not.toContain("B"); // 35 is above under-800 ceiling
    expect(result.map((d) => d.district)).not.toContain("C");
    expect(result.map((d) => d.district)).not.toContain("E");
  });

  it("800-1500 tier excludes under-800 and over-1500 districts", () => {
    const result = filterByBudget(districts, "800-1500");
    expect(result.map((d) => d.district)).toContain("B");    // 35 within [26.7, 50)
    expect(result.map((d) => d.district)).not.toContain("A"); // 15 < 26.7 → excluded
    expect(result.map((d) => d.district)).not.toContain("C"); // 70 >= 50 → excluded
  });

  it("over-2500 includes only most expensive district", () => {
    const result = filterByBudget(districts, "over-2500");
    expect(result.map((d) => d.district)).toContain("D");
    expect(result.map((d) => d.district)).not.toContain("A");
  });

  it("excludes districts with null medianPriceWan", () => {
    const result = filterByBudget(districts, "800-1500");
    expect(result.every((d) => d.medianPriceWan !== null)).toBe(true);
  });

  it("returns empty when no districts qualify", () => {
    const result = filterByBudget([makeDistrict("X", "台北市", null, null)], "under-800");
    expect(result).toHaveLength(0);
  });
});

// ── filterByCommute ───────────────────────────────────────────────────────────

describe("filterByCommute", () => {
  const districts = [nearTaipei, midTaipei, farDistrict, veryFar];

  it("30-min filter keeps close districts, excludes far ones", () => {
    const result = filterByCommute(districts, "taipei-main", 30);
    // 30 min ≈ 15 km; Taipei districts ~few km away should pass; Hsinchu ~80 km should not
    expect(result.map((d) => d.district)).not.toContain("新竹東區");
  });

  it("60-min filter is more permissive than 30-min", () => {
    const r30 = filterByCommute(districts, "taipei-main", 30);
    const r60 = filterByCommute(districts, "taipei-main", 60);
    expect(r60.length).toBeGreaterThanOrEqual(r30.length);
  });

  it("unknown hub id → returns all districts (no filter)", () => {
    const result = filterByCommute(districts, "unknown-hub", 30);
    expect(result).toHaveLength(districts.length);
  });

  it("all COMMUTE_HUBS have valid ids", () => {
    for (const hub of COMMUTE_HUBS) {
      expect(typeof hub.id).toBe("string");
      expect(hub.id.length).toBeGreaterThan(0);
    }
  });
});

// ── sortByPriority ────────────────────────────────────────────────────────────

describe("sortByPriority", () => {
  const districts: DistrictInput[] = [
    makeDistrict("Cheap",    "台北市", 20, 80, 25.04, 121.52, 10),
    makeDistrict("Mid",      "台北市", 50, 50, 25.04, 121.52, 30),
    makeDistrict("Pricey",   "台北市", 90, 20, 25.04, 121.52, 50),
    makeDistrict("NoData",   "台北市", null, null, 25.04, 121.52, 15),
  ];

  it("total-lowest sorts cheapest first, nulls last", () => {
    const sorted = sortByPriority(districts, "total-lowest");
    expect(sorted[0]!.district).toBe("Cheap");
    expect(sorted[sorted.length - 1]!.district).toBe("NoData");
  });

  it("ping-best sorts cheapest first (same as total-lowest)", () => {
    const sorted = sortByPriority(districts, "ping-best");
    expect(sorted[0]!.district).toBe("Cheap");
  });

  it("timing-score sorts highest score first, nulls last", () => {
    const sorted = sortByPriority(districts, "timing-score");
    expect(sorted[0]!.district).toBe("Cheap"); // score 80
    expect(sorted[sorted.length - 1]!.district).toBe("NoData");
  });

  it("newest sorts by txCount12mo descending", () => {
    const sorted = sortByPriority(districts, "newest");
    expect(sorted[0]!.district).toBe("Pricey"); // txCount 50
    expect(sorted[sorted.length - 1]!.district).toBe("Cheap"); // txCount 10
  });

  it("does not mutate the input array", () => {
    const original = [...districts];
    sortByPriority(districts, "total-lowest");
    expect(districts.map((d) => d.district)).toEqual(original.map((d) => d.district));
  });
});

// ── runWizard ─────────────────────────────────────────────────────────────────

describe("runWizard", () => {
  // Build a set of districts: 3 near Taipei in 800-1500 range [26.7, 50), 1 pricey, 1 far
  const districts: DistrictInput[] = [
    makeDistrict("中山區", "台北市", 40, 70, 25.063, 121.53),
    makeDistrict("大同區", "台北市", 35, 80, 25.063, 121.51),
    makeDistrict("萬華區", "台北市", 30, 75, 25.036, 121.5),
    makeDistrict("信義區", "台北市", 95, 30, 25.033, 121.565),
    makeDistrict("高雄某區", "高雄市", 38, 90, 22.627, 120.301),  // near kaohsiung, 38 万/坪 in 800-1500 range
  ];

  const params: WizardParams = {
    budget: "800-1500",
    downPayment: "10-20",
    commuteHub: "taipei-main",
    commuteMins: 45,
    priority: "timing-score",
  };

  it("returns at most 3 matches", () => {
    const { matches } = runWizard(params, districts);
    expect(matches.length).toBeLessThanOrEqual(3);
  });

  it("excludes districts outside budget (信義區 at 95 萬/坪 > 50 ceiling)", () => {
    const { matches } = runWizard(params, districts);
    expect(matches.map((m) => m.district)).not.toContain("信義區");
  });

  it("excludes districts too far from commute hub (高雄)", () => {
    const { matches } = runWizard(params, districts);
    expect(matches.map((m) => m.district)).not.toContain("高雄某區");
  });

  it("matches include distanceKm and hubLabel", () => {
    const { matches } = runWizard(params, districts);
    for (const m of matches) {
      expect(m.distanceKm).not.toBeNull();
      expect(m.distanceKm!).toBeGreaterThanOrEqual(0);
      expect(m.hubLabel).toBe("台北車站");
    }
  });

  it("timing-score priority returns highest-score districts first", () => {
    const { matches } = runWizard(params, districts);
    for (let i = 1; i < matches.length; i++) {
      const prev = matches[i - 1]!.buyerTimingScore ?? -Infinity;
      const curr = matches[i]!.buyerTimingScore ?? -Infinity;
      expect(prev).toBeGreaterThanOrEqual(curr);
    }
  });

  it("needsRelax is false when 3+ matches exist", () => {
    const { needsRelax, matches } = runWizard(params, districts);
    if (matches.length === 3) expect(needsRelax).toBe(false);
  });

  it("edge case: 0 matching districts → empty matches + needsRelax true", () => {
    // Budget "over-2500" requires >= 83.4 萬/坪; commute from kaohsiung-mrt 30min limits to ~15km.
    // All Taipei districts are far from Kaohsiung → filtered by commute.
    // 高雄某區 (38 萬/坪) is near Kaohsiung but below 83.4 → filtered by budget.
    // Result: 0 matches.
    const restrictive: WizardParams = {
      budget: "over-2500",
      downPayment: "under-5",
      commuteHub: "kaohsiung-mrt",
      commuteMins: 30,
      priority: "total-lowest",
    };
    const { matches, needsRelax } = runWizard(restrictive, districts);
    expect(matches).toHaveLength(0);
    expect(needsRelax).toBe(true);
  });

  it("edge case: fewer than 3 matches → needsRelax true", () => {
    const tightParams: WizardParams = {
      budget: "800-1500",
      downPayment: "10-20",
      commuteHub: "taipei-main",
      commuteMins: 30, // tighter commute
      priority: "total-lowest",
    };
    const { needsRelax } = runWizard(tightParams, [
      makeDistrict("只有一個", "台北市", 30, 70, 25.04, 121.52),
    ]);
    expect(needsRelax).toBe(true);
  });

  it("handles empty district list gracefully", () => {
    const { matches, needsRelax } = runWizard(params, []);
    expect(matches).toHaveLength(0);
    expect(needsRelax).toBe(true);
  });
});
