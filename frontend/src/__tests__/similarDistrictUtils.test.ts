import { describe, it, expect } from "vitest";
import {
  computeSimilarDistricts,
  ageBucketFromMedianAge,
  DEFAULT_TIMING_TOLERANCE,
  DEFAULT_MAX_RESULTS,
  type DistrictProfile,
} from "../lib/similarDistrictUtils.js";

// ── Fixtures ─────────────────────────────────────────────────────────────────

/** Build a minimal DistrictProfile; required fields + safe defaults. */
function makeProfile(overrides: Partial<DistrictProfile> & { district: string; city: string }): DistrictProfile {
  return {
    medianPricePing: 500_000,
    buyerTimingScore: 60,
    firstBuyerTier: 1,
    dominantAgeBucket: 2,
    ...overrides,
  };
}

/** The target district used in most tests. */
const TARGET = makeProfile({
  district: "信義區",
  city: "台北市",
  medianPricePing: 1_000_000,
  buyerTimingScore: 60,
  firstBuyerTier: 1,
  dominantAgeBucket: 2,
});

/** Three valid cheaper candidates (well within all thresholds). */
function makeBaseProfiles(): DistrictProfile[] {
  return [
    TARGET,
    makeProfile({ district: "大安區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 55, firstBuyerTier: 1, dominantAgeBucket: 2 }),
    makeProfile({ district: "中山區", city: "台北市", medianPricePing: 700_000, buyerTimingScore: 65, firstBuyerTier: 0, dominantAgeBucket: 3 }),
    makeProfile({ district: "松山區", city: "台北市", medianPricePing: 750_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 1 }),
  ];
}

// ── ageBucketFromMedianAge ────────────────────────────────────────────────────

describe("ageBucketFromMedianAge", () => {
  it("returns 0 for age < 5", () => {
    expect(ageBucketFromMedianAge(0)).toBe(0);
    expect(ageBucketFromMedianAge(4.9)).toBe(0);
  });

  it("returns 1 for 5 ≤ age < 10", () => {
    expect(ageBucketFromMedianAge(5)).toBe(1);
    expect(ageBucketFromMedianAge(9.9)).toBe(1);
  });

  it("returns 2 for 10 ≤ age < 20", () => {
    expect(ageBucketFromMedianAge(10)).toBe(2);
    expect(ageBucketFromMedianAge(19)).toBe(2);
  });

  it("returns 3 for 20 ≤ age < 30", () => {
    expect(ageBucketFromMedianAge(20)).toBe(3);
    expect(ageBucketFromMedianAge(29)).toBe(3);
  });

  it("returns 4 for 30 ≤ age < 40", () => {
    expect(ageBucketFromMedianAge(30)).toBe(4);
    expect(ageBucketFromMedianAge(39)).toBe(4);
  });

  it("returns 5 for age ≥ 40", () => {
    expect(ageBucketFromMedianAge(40)).toBe(5);
    expect(ageBucketFromMedianAge(80)).toBe(5);
  });
});

// ── computeSimilarDistricts — no-match / empty scenarios ─────────────────────

describe("computeSimilarDistricts — edge cases", () => {
  it("returns insufficient=true and empty candidates when allProfiles is empty", () => {
    const result = computeSimilarDistricts("信義區", "台北市", []);
    expect(result.insufficient).toBe(true);
    expect(result.candidates).toHaveLength(0);
  });

  it("returns insufficient=true when target district is not in allProfiles", () => {
    const result = computeSimilarDistricts("不存在區", "台北市", makeBaseProfiles());
    expect(result.insufficient).toBe(true);
    expect(result.candidates).toHaveLength(0);
  });

  it("returns insufficient=true when target has no price data (medianPricePing = -1)", () => {
    const profiles = makeBaseProfiles();
    profiles[0] = { ...TARGET, medianPricePing: -1 };
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.insufficient).toBe(true);
  });

  it("returns insufficient=true when target has medianPricePing = 0", () => {
    const profiles = [{ ...TARGET, medianPricePing: 0 }, ...makeBaseProfiles().slice(1)];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.insufficient).toBe(true);
  });

  it("excludes the target district itself from candidates", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    expect(result.candidates.every((c) => !(c.district === "信義區" && c.city === "台北市"))).toBe(true);
  });

  it("returns insufficient=true when all candidates are more expensive than target", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "大安區", city: "台北市", medianPricePing: 1_200_000 }),
      makeProfile({ district: "中山區", city: "台北市", medianPricePing: 1_100_000 }),
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.insufficient).toBe(true);
    expect(result.candidates).toHaveLength(0);
  });

  it("returns insufficient=true when fewer than 3 qualifying candidates exist", () => {
    const profiles = [TARGET, makeBaseProfiles()[1]!]; // only 1 cheaper candidate
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.insufficient).toBe(true);
    expect(result.candidates).toHaveLength(1);
  });
});

// ── computeSimilarDistricts — basic happy path ────────────────────────────────

describe("computeSimilarDistricts — happy path", () => {
  it("returns 3 candidates when ≥ 3 qualify", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    expect(result.candidates).toHaveLength(3);
    expect(result.insufficient).toBe(false);
  });

  it("ranks candidates by discountPct descending (largest discount first)", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    const discounts = result.candidates.map((c) => c.discountPct);
    for (let i = 1; i < discounts.length; i++) {
      expect(discounts[i - 1]!).toBeGreaterThanOrEqual(discounts[i]!);
    }
  });

  it("computes discountPct correctly", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    // 中山區: 700k / 1000k → 30% discount
    const zhongshan = result.candidates.find((c) => c.district === "中山區");
    expect(zhongshan).toBeDefined();
    expect(zhongshan!.discountPct).toBeCloseTo(30, 1);
  });

  it("includes correct fields on each candidate", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    for (const c of result.candidates) {
      expect(c).toHaveProperty("district");
      expect(c).toHaveProperty("city");
      expect(c).toHaveProperty("medianPricePing");
      expect(c).toHaveProperty("discountPct");
      expect(c).toHaveProperty("buyerTimingScore");
      expect(c).toHaveProperty("firstBuyerTier");
    }
  });

  it("respects maxResults option", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles(), { maxResults: 2 });
    expect(result.candidates.length).toBeLessThanOrEqual(2);
  });
});

// ── Timing-score filter ───────────────────────────────────────────────────────

describe("computeSimilarDistricts — timing-score filter", () => {
  it("excludes candidates whose score differs by more than tolerance (default 15)", () => {
    const profiles = [
      TARGET, // score 60
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 40 }), // diff = 20 → excluded
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 75 }), // diff = 15 → included
      makeProfile({ district: "C區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 76 }), // diff = 16 → excluded
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(false);
    expect(result.candidates.some((c) => c.district === "B區")).toBe(true);
    expect(result.candidates.some((c) => c.district === "C區")).toBe(false);
  });

  it("includes candidate when target.buyerTimingScore is null (skip dimension)", () => {
    const target = { ...TARGET, buyerTimingScore: null };
    const profiles = [
      target,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 500_000, buyerTimingScore: 10 }), // would be excluded if target score = 60
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });

  it("includes candidate when candidate.buyerTimingScore is null (skip dimension)", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 500_000, buyerTimingScore: null }), // skip
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });

  it("respects custom timingScoreTolerance option", () => {
    const profiles = [
      TARGET, // score 60
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 55 }), // diff = 5 → pass
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 800_000, buyerTimingScore: 50 }), // diff = 10 → fail with tolerance=8
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles, { timingScoreTolerance: 8 });
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
    expect(result.candidates.some((c) => c.district === "B區")).toBe(false);
  });
});

// ── Affordability tier filter ─────────────────────────────────────────────────

describe("computeSimilarDistricts — affordability tier filter", () => {
  it("includes candidate with same affordability tier as target", () => {
    const profiles = [
      TARGET, // tier 1
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, firstBuyerTier: 1 }),
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 750_000, firstBuyerTier: 1 }),
      makeProfile({ district: "C區", city: "台北市", medianPricePing: 700_000, firstBuyerTier: 1 }),
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates).toHaveLength(3);
  });

  it("includes candidate with better (lower) affordability tier", () => {
    const profiles = [
      TARGET, // tier 1
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, firstBuyerTier: 0 }), // tier 0 = better → include
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });

  it("excludes candidate with worse affordability tier", () => {
    const profiles = [
      TARGET, // tier 1 (stretch)
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, firstBuyerTier: 2 }), // outOfRange → worse
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(false);
  });

  it("skips tier filter when target.firstBuyerTier is -1 (no data)", () => {
    const target = { ...TARGET, firstBuyerTier: -1 as const };
    const profiles = [
      target,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, firstBuyerTier: 2 }), // would be excluded normally
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });

  it("skips tier filter when candidate.firstBuyerTier is -1 (no data)", () => {
    const profiles = [
      TARGET, // tier 1
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, firstBuyerTier: -1 }), // no data → skip filter
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });
});

// ── Dominant age bucket filter ────────────────────────────────────────────────

describe("computeSimilarDistricts — age bucket filter", () => {
  it("includes candidate whose dominant bucket is within ±1 of target", () => {
    const profiles = [
      TARGET, // bucket 2
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, dominantAgeBucket: 1 }), // diff=1 → OK
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 750_000, dominantAgeBucket: 3 }), // diff=1 → OK
      makeProfile({ district: "C區", city: "台北市", medianPricePing: 700_000, dominantAgeBucket: 2 }), // diff=0 → OK
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates).toHaveLength(3);
  });

  it("excludes candidate whose dominant bucket differs by more than 1", () => {
    const profiles = [
      TARGET, // bucket 2
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, dominantAgeBucket: 0 }), // diff=2 → excluded
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 750_000, dominantAgeBucket: 4 }), // diff=2 → excluded
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates).toHaveLength(0);
    expect(result.insufficient).toBe(true);
  });

  it("skips age filter when target.dominantAgeBucket is null", () => {
    const target = { ...TARGET, dominantAgeBucket: null };
    const profiles = [
      target,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, dominantAgeBucket: 0 }), // would be excluded
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });

  it("skips age filter when candidate.dominantAgeBucket is null", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000, dominantAgeBucket: null }),
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(true);
  });
});

// ── City filter ────────────────────────────────────────────────────────────────

describe("computeSimilarDistricts — city filter", () => {
  it("does not filter by city when sameCityOnly=false (default)", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "三民區", city: "高雄市", medianPricePing: 400_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 2 }),
      makeProfile({ district: "北屯區", city: "台中市", medianPricePing: 450_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 2 }),
      makeProfile({ district: "東區",   city: "台南市", medianPricePing: 350_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 2 }),
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates).toHaveLength(3);
    expect(result.candidates.some((c) => c.city === "高雄市")).toBe(true);
  });

  it("restricts to same city when sameCityOnly=true", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "大安區", city: "台北市", medianPricePing: 900_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 2 }),
      makeProfile({ district: "三民區", city: "高雄市", medianPricePing: 400_000, buyerTimingScore: 60, firstBuyerTier: 1, dominantAgeBucket: 2 }),
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles, { sameCityOnly: true });
    expect(result.candidates.every((c) => c.city === "台北市")).toBe(true);
    expect(result.candidates.some((c) => c.district === "三民區")).toBe(false);
  });
});

// ── Tie-breaking and exact-match tests ───────────────────────────────────────

describe("computeSimilarDistricts — ties and precision", () => {
  it("handles two candidates with identical prices (zero discount excluded — not cheaper)", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 1_000_000 }), // same price → not cheaper
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates.some((c) => c.district === "A區")).toBe(false);
  });

  it("handles candidates with equal discounts (stable ordering — top 3 all included)", () => {
    const profiles = [
      TARGET,
      makeProfile({ district: "A區", city: "台北市", medianPricePing: 800_000 }), // 20% discount
      makeProfile({ district: "B區", city: "台北市", medianPricePing: 800_000 }), // 20% discount
      makeProfile({ district: "C區", city: "台北市", medianPricePing: 800_000 }), // 20% discount
    ];
    const result = computeSimilarDistricts("信義區", "台北市", profiles);
    expect(result.candidates).toHaveLength(3);
    expect(result.insufficient).toBe(false);
  });

  it("discountPct is a finite positive number", () => {
    const result = computeSimilarDistricts("信義區", "台北市", makeBaseProfiles());
    for (const c of result.candidates) {
      expect(isFinite(c.discountPct)).toBe(true);
      expect(c.discountPct).toBeGreaterThan(0);
    }
  });
});

// ── DEFAULT constant checks ───────────────────────────────────────────────────

describe("constants", () => {
  it("DEFAULT_TIMING_TOLERANCE is 15", () => {
    expect(DEFAULT_TIMING_TOLERANCE).toBe(15);
  });

  it("DEFAULT_MAX_RESULTS is 3", () => {
    expect(DEFAULT_MAX_RESULTS).toBe(3);
  });
});
