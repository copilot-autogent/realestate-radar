import { describe, it, expect } from "vitest";
import {
  rawFacilitiesScore,
  normalizeScores,
  computeAllFacilitiesScores,
  facilitiesTier,
  facilitiesBadge,
  type PoiCounts,
} from "../lib/facilitiesUtils";

const ZERO_COUNTS: PoiCounts = {
  restaurant: 0, sport: 0, cinema: 0,
  supermarket: 0, convenience: 0, park: 0, hospital: 0,
};

const RICH_COUNTS: PoiCounts = {
  restaurant: 200, sport: 30, cinema: 8,
  supermarket: 50, convenience: 120, park: 60, hospital: 90,
};

const MID_COUNTS: PoiCounts = {
  restaurant: 40, sport: 8, cinema: 1,
  supermarket: 10, convenience: 25, park: 15, hospital: 20,
};

describe("rawFacilitiesScore", () => {
  it("returns 0 for zero counts", () => {
    expect(rawFacilitiesScore(ZERO_COUNTS)).toBe(0);
  });

  it("returns positive value for non-zero counts", () => {
    expect(rawFacilitiesScore(RICH_COUNTS)).toBeGreaterThan(0);
  });

  it("scores rich district higher than mid district", () => {
    expect(rawFacilitiesScore(RICH_COUNTS)).toBeGreaterThan(rawFacilitiesScore(MID_COUNTS));
  });

  it("scores mid district higher than zero", () => {
    expect(rawFacilitiesScore(MID_COUNTS)).toBeGreaterThan(rawFacilitiesScore(ZERO_COUNTS));
  });

  it("convenience_store has highest weight impact", () => {
    const convOnly: PoiCounts = { ...ZERO_COUNTS, convenience: 80 };
    const restOnly: PoiCounts = { ...ZERO_COUNTS, restaurant: 80 };
    expect(rawFacilitiesScore(convOnly)).toBeGreaterThan(rawFacilitiesScore(restOnly));
  });
});

describe("normalizeScores", () => {
  it("returns empty array for empty input", () => {
    expect(normalizeScores([])).toEqual([]);
  });

  it("all-same scores → all 50", () => {
    expect(normalizeScores([42, 42, 42])).toEqual([50, 50, 50]);
  });

  it("min gets 0, max gets 100", () => {
    const result = normalizeScores([0, 50, 100]);
    expect(result[0]).toBe(0);
    expect(result[2]).toBe(100);
    expect(result[1]).toBeGreaterThan(0);
    expect(result[1]).toBeLessThan(100);
  });

  it("single element → 50", () => {
    expect(normalizeScores([999])).toEqual([50]);
  });

  it("all output values are in [0, 100]", () => {
    const result = normalizeScores([10, 30, 5, 80, 45]);
    result.forEach(v => {
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(100);
    });
  });
});

describe("computeAllFacilitiesScores", () => {
  it("returns empty object for empty input", () => {
    expect(computeAllFacilitiesScores({})).toEqual({});
  });

  it("highest-raw-score district gets score 100", () => {
    const input = {
      "台北市::大安區": RICH_COUNTS,
      "台北市::中正區": MID_COUNTS,
      "桃園市::復興區": ZERO_COUNTS,
    };
    const result = computeAllFacilitiesScores(input);
    expect(result["台北市::大安區"]).toBe(100);
    expect(result["桃園市::復興區"]).toBe(0);
    expect(result["台北市::中正區"]).toBeGreaterThan(0);
    expect(result["台北市::中正區"]).toBeLessThan(100);
  });

  it("all scores are integers in [0, 100]", () => {
    const input = {
      "a": RICH_COUNTS,
      "b": MID_COUNTS,
      "c": ZERO_COUNTS,
    };
    const result = computeAllFacilitiesScores(input);
    Object.values(result).forEach(v => {
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(100);
      expect(Number.isInteger(v)).toBe(true);
    });
  });
});

describe("facilitiesTier", () => {
  it("75+ → excellent", () => {
    expect(facilitiesTier(75)).toBe("excellent");
    expect(facilitiesTier(100)).toBe("excellent");
  });
  it("50–74 → good", () => {
    expect(facilitiesTier(50)).toBe("good");
    expect(facilitiesTier(74)).toBe("good");
  });
  it("25–49 → fair", () => {
    expect(facilitiesTier(25)).toBe("fair");
    expect(facilitiesTier(49)).toBe("fair");
  });
  it("0–24 → poor", () => {
    expect(facilitiesTier(0)).toBe("poor");
    expect(facilitiesTier(24)).toBe("poor");
  });
});

describe("facilitiesBadge", () => {
  it("includes score number in output", () => {
    expect(facilitiesBadge(80)).toContain("80");
    expect(facilitiesBadge(30)).toContain("30");
  });

  it("includes emoji for each tier", () => {
    expect(facilitiesBadge(80)).toContain("🟢");
    expect(facilitiesBadge(60)).toContain("🟡");
    expect(facilitiesBadge(35)).toContain("🟠");
    expect(facilitiesBadge(10)).toContain("🔴");
  });
});
