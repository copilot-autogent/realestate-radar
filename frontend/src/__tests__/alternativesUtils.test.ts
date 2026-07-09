/**
 * Tests for alternativesUtils (issue #163): smart same-city alternatives.
 */

import { describe, it, expect } from "vitest";
import { computeAlternatives, ALTERNATIVES_MAX_RESULTS } from "../lib/alternativesUtils.js";
import type { DistrictProfile } from "../lib/similarDistrictUtils.js";

// ── Fixtures ──────────────────────────────────────────────────────────────────

function makeProfile(
  district: string,
  city: string,
  medianPricePing: number,
): DistrictProfile {
  return {
    district,
    city,
    medianPricePing,
    buyerTimingScore: 60,
    firstBuyerTier: 1,
    dominantAgeBucket: 2,
  };
}

const TAIPEI_PROFILES: DistrictProfile[] = [
  makeProfile("大安區", "台北市", 720_000),   // target
  makeProfile("文山區", "台北市", 380_000),   // cheaper
  makeProfile("南港區", "台北市", 450_000),   // cheaper
  makeProfile("中正區", "台北市", 490_000),   // cheaper
  makeProfile("士林區", "台北市", 560_000),   // cheaper
  makeProfile("信義區", "台北市", 810_000),   // more expensive — excluded
  makeProfile("板橋區", "新北市", 300_000),   // different city — excluded
];

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("computeAlternatives", () => {
  it("returns cheaper same-city districts sorted ascending by price", () => {
    const { candidates, targetInsufficient } = computeAlternatives(
      "大安區",
      "台北市",
      TAIPEI_PROFILES,
    );

    expect(targetInsufficient).toBe(false);
    expect(candidates.length).toBeGreaterThan(0);

    // All must be same city
    for (const c of candidates) {
      expect(c.city).toBe("台北市");
    }

    // All must be cheaper than target (720,000)
    for (const c of candidates) {
      expect(c.medianPricePing).toBeLessThan(720_000);
    }

    // Sorted ascending
    for (let i = 1; i < candidates.length; i++) {
      expect(candidates[i]!.medianPricePing).toBeGreaterThanOrEqual(candidates[i - 1]!.medianPricePing);
    }

    // Cheapest first = 文山區
    expect(candidates[0]!.district).toBe("文山區");
  });

  it("excludes the target district itself", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES);
    const names = candidates.map((c) => c.district);
    expect(names).not.toContain("大安區");
  });

  it("excludes districts from other cities", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES);
    const names = candidates.map((c) => c.district);
    expect(names).not.toContain("板橋區");
  });

  it("excludes more-expensive districts (信義區)", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES);
    const names = candidates.map((c) => c.district);
    expect(names).not.toContain("信義區");
  });

  it("computes priceDeltaPct correctly", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES);
    // 文山區: (720000 - 380000) / 720000 * 100 ≈ 47.2%
    const wenshan = candidates.find((c) => c.district === "文山區");
    expect(wenshan).toBeDefined();
    expect(wenshan!.priceDeltaPct).toBeCloseTo(47.2, 0);
  });

  it("honours maxResults cap", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES, {}, 2);
    expect(candidates.length).toBeLessThanOrEqual(2);
  });

  it("respects ALTERNATIVES_MAX_RESULTS default", () => {
    const many: DistrictProfile[] = Array.from({ length: 10 }, (_, i) =>
      makeProfile(`區${i}`, "台北市", (i + 1) * 50_000),
    );
    many.push(makeProfile("大安區", "台北市", 800_000));
    const { candidates } = computeAlternatives("大安區", "台北市", many);
    expect(candidates.length).toBeLessThanOrEqual(ALTERNATIVES_MAX_RESULTS);
  });

  it("returns targetInsufficient=true when target has no data", () => {
    const profiles = [makeProfile("大安區", "台北市", -1), ...TAIPEI_PROFILES.slice(1)];
    const { targetInsufficient, candidates } = computeAlternatives("大安區", "台北市", profiles);
    expect(targetInsufficient).toBe(true);
    expect(candidates.length).toBe(0);
  });

  it("returns targetInsufficient=true when target district not found", () => {
    const { targetInsufficient } = computeAlternatives("不存在區", "台北市", TAIPEI_PROFILES);
    expect(targetInsufficient).toBe(true);
  });

  it("returns empty candidates when cheapest is the only option and it costs the same", () => {
    const profiles = [
      makeProfile("A區", "台北市", 500_000),
      makeProfile("B區", "台北市", 500_000), // same price — not strictly cheaper
    ];
    const { candidates } = computeAlternatives("A區", "台北市", profiles);
    expect(candidates.length).toBe(0);
  });

  it("attaches MRT tier from mrtTierMap", () => {
    const mrtTierMap = {
      "台北市:文山區": { tier: "nearby" as const, noMrt: false },
      "台北市:南港區": { tier: "walking" as const, noMrt: false },
    };
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES, mrtTierMap);
    const wenshan = candidates.find((c) => c.district === "文山區");
    const nangang = candidates.find((c) => c.district === "南港區");
    expect(wenshan?.mrtTier).toBe("nearby");
    expect(nangang?.mrtTier).toBe("walking");
  });

  it("sets mrtTier=null when district not in mrtTierMap", () => {
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES, {});
    for (const c of candidates) {
      expect(c.mrtTier).toBeNull();
    }
  });

  it("sets noMrtCoverage=true for districts without MRT", () => {
    const mrtTierMap = {
      "台北市:文山區": { tier: "remote" as const, noMrt: true },
    };
    const { candidates } = computeAlternatives("大安區", "台北市", TAIPEI_PROFILES, mrtTierMap);
    const wenshan = candidates.find((c) => c.district === "文山區");
    expect(wenshan?.noMrtCoverage).toBe(true);
  });
});
