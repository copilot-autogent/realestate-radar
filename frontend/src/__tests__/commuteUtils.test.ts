import { describe, it, expect } from "vitest";
import {
  haversineKm,
  computeAnchorCommuteScore,
  findNearestHub,
  computeTransitProximityScore,
  commuteScoreLabel,
  commuteBadge,
  groupHubsByCity,
  TRANSIT_HUBS,
  type TransitHub,
} from "../lib/commuteUtils";

const TAIPEI_MAIN: TransitHub = {
  id: "taipei-main",
  label: "台北車站 (MRT/TRA/HSR)",
  lat: 25.0478,
  lng: 121.5170,
  type: "mrt",
  city: "台北市",
};

const BANQIAO: TransitHub = {
  id: "banqiao",
  label: "板橋站 (MRT/TRA/HSR)",
  lat: 25.0142,
  lng: 121.4633,
  type: "hsr",
  city: "新北市",
};

const HSR_TAOYUAN: TransitHub = {
  id: "hsr-taoyuan",
  label: "高鐵桃園站",
  lat: 24.9978,
  lng: 121.2333,
  type: "hsr",
  city: "桃園市",
};

// A mock district centroid for 大安區 (approximately)
const DAAN_LAT = 25.026;
const DAAN_LNG = 121.543;

describe("haversineKm", () => {
  it("returns 0 for identical points", () => {
    expect(haversineKm(25.0, 121.5, 25.0, 121.5)).toBeCloseTo(0, 5);
  });

  it("approx correct distance Taipei Main to Banqiao (~8 km)", () => {
    const d = haversineKm(
      TAIPEI_MAIN.lat, TAIPEI_MAIN.lng,
      BANQIAO.lat,     BANQIAO.lng,
    );
    expect(d).toBeGreaterThan(5);
    expect(d).toBeLessThan(12);
  });

  it("is symmetric", () => {
    const d1 = haversineKm(25.0, 121.0, 25.1, 121.1);
    const d2 = haversineKm(25.1, 121.1, 25.0, 121.0);
    expect(d1).toBeCloseTo(d2, 5);
  });
});

describe("computeAnchorCommuteScore", () => {
  it("returns 100 when district is exactly at anchor", () => {
    expect(computeAnchorCommuteScore(TAIPEI_MAIN.lat, TAIPEI_MAIN.lng, TAIPEI_MAIN)).toBe(100);
  });

  it("returns ~50 at 8 km (half-life)", () => {
    // 大安 is roughly 3 km from Taipei Main. Let's use a point far away:
    // At exactly 8 km the score should be ~50
    const score = computeAnchorCommuteScore(TAIPEI_MAIN.lat, TAIPEI_MAIN.lng + 0.0878, TAIPEI_MAIN);
    // 0.0878 degrees ≈ ~8 km longitude at 25° latitude
    expect(score).toBeGreaterThan(30);
    expect(score).toBeLessThan(70);
  });

  it("score decreases as distance increases", () => {
    const close = computeAnchorCommuteScore(DAAN_LAT, DAAN_LNG, TAIPEI_MAIN);
    const far = computeAnchorCommuteScore(HSR_TAOYUAN.lat, HSR_TAOYUAN.lng, TAIPEI_MAIN);
    expect(close).toBeGreaterThan(far);
  });

  it("all values in [0, 100]", () => {
    const scores = [
      computeAnchorCommuteScore(DAAN_LAT, DAAN_LNG, TAIPEI_MAIN),
      computeAnchorCommuteScore(BANQIAO.lat, BANQIAO.lng, TAIPEI_MAIN),
      computeAnchorCommuteScore(HSR_TAOYUAN.lat, HSR_TAOYUAN.lng, TAIPEI_MAIN),
    ];
    scores.forEach(s => {
      expect(s).toBeGreaterThanOrEqual(0);
      expect(s).toBeLessThanOrEqual(100);
      expect(Number.isInteger(s)).toBe(true);
    });
  });
});

describe("findNearestHub", () => {
  const hubs = [TAIPEI_MAIN, BANQIAO, HSR_TAOYUAN];

  it("returns null for empty hubs", () => {
    expect(findNearestHub(DAAN_LAT, DAAN_LNG, [])).toBeNull();
  });

  it("returns the geographically nearest hub", () => {
    // 大安區 is closest to Taipei Main among the three
    const result = findNearestHub(DAAN_LAT, DAAN_LNG, hubs);
    expect(result).not.toBeNull();
    expect(result!.hub.id).toBe("taipei-main");
  });

  it("distance is non-negative", () => {
    const result = findNearestHub(DAAN_LAT, DAAN_LNG, hubs);
    expect(result!.distanceKm).toBeGreaterThanOrEqual(0);
  });
});

describe("computeTransitProximityScore", () => {
  it("returns score 0 and null hub for empty hubs", () => {
    const r = computeTransitProximityScore(DAAN_LAT, DAAN_LNG, []);
    expect(r.score).toBe(0);
    expect(r.nearestHub).toBeNull();
    expect(r.distanceKm).toBe(Infinity);
  });

  it("score is 100 when district is at a hub", () => {
    const r = computeTransitProximityScore(TAIPEI_MAIN.lat, TAIPEI_MAIN.lng, [TAIPEI_MAIN]);
    expect(r.score).toBe(100);
  });

  it("correctly identifies nearest hub", () => {
    const r = computeTransitProximityScore(DAAN_LAT, DAAN_LNG, [TAIPEI_MAIN, BANQIAO, HSR_TAOYUAN]);
    expect(r.nearestHub?.id).toBe("taipei-main");
    expect(r.distanceKm).toBeLessThan(5);
  });
});

describe("commuteScoreLabel", () => {
  it("80+ → 通勤便利", () => {
    expect(commuteScoreLabel(80)).toBe("通勤便利");
    expect(commuteScoreLabel(100)).toBe("通勤便利");
  });
  it("55–79 → 通勤尚可", () => {
    expect(commuteScoreLabel(55)).toBe("通勤尚可");
    expect(commuteScoreLabel(79)).toBe("通勤尚可");
  });
  it("30–54 → 需開車通勤", () => {
    expect(commuteScoreLabel(30)).toBe("需開車通勤");
    expect(commuteScoreLabel(54)).toBe("需開車通勤");
  });
  it("<30 → 通勤偏遠", () => {
    expect(commuteScoreLabel(0)).toBe("通勤偏遠");
    expect(commuteScoreLabel(29)).toBe("通勤偏遠");
  });
});

describe("commuteBadge", () => {
  it("includes score, hub label and distance", () => {
    const badge = commuteBadge(88, TAIPEI_MAIN, 2.3);
    expect(badge).toContain("88");
    expect(badge).toContain("台北車站");
    expect(badge).toContain("2.3");
  });

  it("MRT hub gets 🚇 emoji", () => {
    expect(commuteBadge(70, TAIPEI_MAIN, 3.0)).toContain("🚇");
  });

  it("HSR hub gets 🚄 emoji", () => {
    expect(commuteBadge(70, BANQIAO, 3.0)).toContain("🚄");
  });
});

describe("groupHubsByCity", () => {
  it("returns empty object for empty input", () => {
    expect(groupHubsByCity([])).toEqual({});
  });

  it("groups by city correctly", () => {
    const hubs = [TAIPEI_MAIN, BANQIAO, HSR_TAOYUAN];
    const groups = groupHubsByCity(hubs);
    expect(Object.keys(groups)).toContain("台北市");
    expect(Object.keys(groups)).toContain("新北市");
    expect(Object.keys(groups)).toContain("桃園市");
    expect(groups["台北市"]!.length).toBe(1);
  });

  it("hubs without city go into 其他", () => {
    const noCity: TransitHub = { id: "x", label: "X", lat: 0, lng: 0, type: "mrt" };
    const groups = groupHubsByCity([noCity]);
    expect(groups["其他"]).toBeDefined();
  });
});

describe("TRANSIT_HUBS catalogue", () => {
  it("has at least 20 hubs", () => {
    expect(TRANSIT_HUBS.length).toBeGreaterThanOrEqual(20);
  });

  it("includes hubs for all 3 target cities", () => {
    const cities = TRANSIT_HUBS.map(h => h.city);
    expect(cities).toContain("台北市");
    expect(cities).toContain("新北市");
    expect(cities).toContain("桃園市");
  });

  it("all hubs have valid lat/lng", () => {
    TRANSIT_HUBS.forEach(h => {
      expect(h.lat).toBeGreaterThan(20);  // Taiwan is ~22-26°N
      expect(h.lat).toBeLessThan(30);
      expect(h.lng).toBeGreaterThan(118); // Taiwan is ~120-122°E
      expect(h.lng).toBeLessThan(126);
    });
  });

  it("all hub ids are unique", () => {
    const ids = TRANSIT_HUBS.map(h => h.id);
    const unique = new Set(ids);
    expect(unique.size).toBe(ids.length);
  });
});
