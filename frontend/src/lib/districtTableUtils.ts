/**
 * Pure utility functions for the district comparison table (issue #113).
 * No DOM/window dependencies — fully testable with Vitest.
 */

export interface DistrictRow {
  district: string;
  city: string;
  medianPriceWan: number | null;    // NT$/坪 in 萬, null = insufficient data
  yoyPct: number | null;            // year-over-year % change, null = insufficient data
  txCount12mo: number;              // transaction count in last 12 months
  buyerTimingScore: number | null;  // 0–100 from #70 logic, null = insufficient data
  sparkline6mo: (number | null)[];  // 6 monthly median unit prices (萬/坪), oldest→newest
}

export type SortKey = "district" | "medianPrice" | "yoyPct" | "txCount" | "timingScore";
export type SortDir = "asc" | "desc";

// ── Helpers ──────────────────────────────────────────────────────────────────

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[mid - 1]! + s[mid]!) / 2 : s[mid]!;
}

function clamp(v: number): number {
  return Math.max(0, Math.min(100, v));
}

/** Map a percentage change to 0–100 sub-score (declining = high score). Range: -20% → 100, +20% → 0 */
function pctToScore(pct: number): number {
  return clamp((-pct + 20) / 40 * 100);
}

// ── Core per-district computations ───────────────────────────────────────────

/**
 * Computes buyer timing score (0–100) for a district.
 * Ported from Map.astro `computeBuyerTimingScore`.
 * Returns null when < MIN_TX_12MO transactions exist in last 12 months.
 */
export function computeBuyerTimingScore(
  features: any[],
  district: string,
  city?: string
): number | null {
  const MIN_TX_12MO = 10;

  const distFeatures = features.filter(
    (f) =>
      f.properties?.district === district &&
      (f.properties?.unitPrice as number) > 0 &&
      (!city || f.properties?.city === city)
  );

  const validTimes = distFeatures
    .map((f) => new Date((f.properties?.date as string) ?? "").getTime())
    .filter((t) => !isNaN(t));

  if (validTimes.length === 0) return null;

  const maxTime = validTimes.reduce((a, b) => (a > b ? a : b), -Infinity);
  const cut12mo = new Date(maxTime);
  cut12mo.setFullYear(cut12mo.getFullYear() - 1);
  const cut24mo = new Date(maxTime);
  cut24mo.setFullYear(cut24mo.getFullYear() - 2);
  const cut36mo = new Date(maxTime);
  cut36mo.setFullYear(cut36mo.getFullYear() - 3);

  const toTime = (f: any) => new Date((f.properties?.date as string) ?? "").getTime();

  const recent12 = distFeatures.filter((f) => {
    const t = toTime(f);
    return !isNaN(t) && t > cut12mo.getTime() && t <= maxTime;
  });
  if (recent12.length < MIN_TX_12MO) return null;

  const prior12 = distFeatures.filter((f) => {
    const t = toTime(f);
    return !isNaN(t) && t > cut24mo.getTime() && t <= cut12mo.getTime();
  });

  // Signal 1: YoY median price change (40%)
  let s1 = 50;
  if (prior12.length >= 3 && recent12.length >= 3) {
    const priorPrices  = prior12.map((f) => f.properties.unitPrice as number);
    const recentPrices = recent12.map((f) => f.properties.unitPrice as number);
    const priorMedian = median(priorPrices);
    if (priorMedian > 0) {
      const pct = ((median(recentPrices) - priorMedian) / priorMedian) * 100;
      s1 = pctToScore(pct);
    }
  }

  // Signal 2: YoY volume change (30%)
  let s2 = 50;
  if (prior12.length >= 3) {
    const pct = ((recent12.length - prior12.length) / prior12.length) * 100;
    s2 = pctToScore(pct);
  }

  // Signal 3: current price vs 3-yr district average (20%)
  let s3 = 50;
  const all3yr = distFeatures.filter((f) => {
    const t = toTime(f);
    return !isNaN(t) && t > cut36mo.getTime() && t <= maxTime;
  });
  if (all3yr.length >= 10) {
    const recentPrices = recent12.map((f) => f.properties.unitPrice as number);
    const avg3yr = all3yr.map((f) => f.properties.unitPrice as number).reduce((a, b) => a + b, 0) / all3yr.length;
    const recentMedian = median(recentPrices);
    const deviation = ((recentMedian - avg3yr) / avg3yr) * 100;
    s3 = pctToScore(deviation);
  }

  // Signal 4: months since local price peak (10%)
  let s4 = 50;
  if (all3yr.length >= 5) {
    const qBuckets: Record<string, number[]> = {};
    for (const f of all3yr) {
      const t = toTime(f);
      if (isNaN(t)) continue;
      const dt = new Date(t);
      const q = Math.floor(dt.getMonth() / 3) + 1;
      const key = `${dt.getFullYear()}-Q${q}`;
      (qBuckets[key] ??= []).push(f.properties.unitPrice as number);
    }
    const sortedQKeys = Object.keys(qBuckets).sort();
    if (sortedQKeys.length >= 2) {
      let peakMedian = -Infinity;
      let peakQKey = sortedQKeys[0]!;
      for (const k of sortedQKeys) {
        const m = median(qBuckets[k]!);
        if (m > peakMedian) { peakMedian = m; peakQKey = k; }
      }
      const [yr, qStr] = peakQKey.split("-Q");
      const peakDate = new Date(Number(yr), (Number(qStr) - 1) * 3 + 1, 15);
      const monthsSincePeak = (maxTime - peakDate.getTime()) / (1000 * 60 * 60 * 24 * 30.44);
      s4 = clamp(monthsSincePeak / 36 * 100);
    }
  }

  return Math.round(0.4 * s1 + 0.3 * s2 + 0.2 * s3 + 0.1 * s4);
}

/**
 * Computes 6-month sparkline data for a district.
 * Returns an array of 6 monthly median unit prices (萬/坪), oldest first.
 * Months with no transactions are null.
 * Anchored to the most recent month with data in the district.
 */
export function compute6MonthSparkline(
  features: any[],
  district: string,
  city?: string
): (number | null)[] {
  const distFeatures = features.filter(
    (f) =>
      f.properties?.district === district &&
      (f.properties?.unitPrice as number) > 0 &&
      (!city || f.properties?.city === city)
  );

  // Group prices by YYYY-MM
  const monthMap: Record<string, number[]> = {};
  for (const f of distFeatures) {
    const d = f.properties?.date;
    if (typeof d !== "string" || d.length < 7) continue;
    const key = d.slice(0, 7);
    if (!/^\d{4}-\d{2}$/.test(key)) continue;
    (monthMap[key] ??= []).push(f.properties.unitPrice as number);
  }

  const allMonthKeys = Object.keys(monthMap).sort();
  if (allMonthKeys.length === 0) return Array(6).fill(null);

  const lastKey = allMonthKeys[allMonthKeys.length - 1]!;
  const [lastYr, lastMo] = lastKey.split("-").map(Number);
  const anchorDate = new Date(lastYr!, lastMo! - 1, 1);

  const result: (number | null)[] = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date(anchorDate);
    d.setMonth(d.getMonth() - i);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    const prices = monthMap[key];
    if (prices && prices.length > 0) {
      result.push(median(prices) / 10_000); // convert to 萬/坪
    } else {
      result.push(null);
    }
  }
  return result;
}

/**
 * Compute YoY price change percentage for a district.
 * Anchored to the latest transaction date. Returns null if insufficient data (< 3 tx per window).
 */
export function computeYoYPct(
  features: any[],
  district: string,
  city?: string
): number | null {
  const distFeatures = features.filter(
    (f) =>
      f.properties?.district === district &&
      (f.properties?.unitPrice as number) > 0 &&
      (!city || f.properties?.city === city)
  );

  const validTimes = distFeatures
    .map((f) => new Date((f.properties?.date as string) ?? "").getTime())
    .filter((t) => !isNaN(t));

  if (validTimes.length === 0) return null;

  const maxTime = validTimes.reduce((a, b) => (a > b ? a : b), -Infinity);
  const cut12mo = new Date(maxTime);
  cut12mo.setFullYear(cut12mo.getFullYear() - 1);
  const cut24mo = new Date(maxTime);
  cut24mo.setFullYear(cut24mo.getFullYear() - 2);

  const toTime = (f: any) => new Date((f.properties?.date as string) ?? "").getTime();

  const recent12 = distFeatures.filter((f) => {
    const t = toTime(f);
    return !isNaN(t) && t > cut12mo.getTime() && t <= maxTime;
  });
  const prior12 = distFeatures.filter((f) => {
    const t = toTime(f);
    return !isNaN(t) && t > cut24mo.getTime() && t <= cut12mo.getTime();
  });

  if (recent12.length < 3 || prior12.length < 3) return null;

  const recentPrices = recent12.map((f) => f.properties.unitPrice as number);
  const priorPrices  = prior12.map((f) => f.properties.unitPrice as number);
  return ((median(recentPrices) - median(priorPrices)) / median(priorPrices)) * 100;
}

/**
 * Compute median unit price (萬/坪) over last 12 months for a district.
 * Returns null if no valid transactions.
 */
export function computeMedianPrice12mo(
  features: any[],
  district: string,
  city?: string
): number | null {
  const distFeatures = features.filter(
    (f) =>
      f.properties?.district === district &&
      (f.properties?.unitPrice as number) > 0 &&
      (!city || f.properties?.city === city)
  );

  const validTimes = distFeatures
    .map((f) => new Date((f.properties?.date as string) ?? "").getTime())
    .filter((t) => !isNaN(t));

  if (validTimes.length === 0) return null;

  const maxTime = validTimes.reduce((a, b) => (a > b ? a : b), -Infinity);
  const cut12mo = new Date(maxTime);
  cut12mo.setFullYear(cut12mo.getFullYear() - 1);

  const recent12 = distFeatures.filter((f) => {
    const t = new Date((f.properties?.date as string) ?? "").getTime();
    return !isNaN(t) && t > cut12mo.getTime() && t <= maxTime;
  });

  if (recent12.length === 0) return null;

  const prices = recent12.map((f) => f.properties.unitPrice as number);
  return median(prices) / 10_000;
}

/**
 * Count transactions in last 12 months for a district.
 */
export function countTx12mo(
  features: any[],
  district: string,
  city?: string
): number {
  const distFeatures = features.filter(
    (f) =>
      f.properties?.district === district &&
      (f.properties?.unitPrice as number) > 0 &&
      (!city || f.properties?.city === city)
  );

  const validTimes = distFeatures
    .map((f) => new Date((f.properties?.date as string) ?? "").getTime())
    .filter((t) => !isNaN(t));

  if (validTimes.length === 0) return 0;

  const maxTime = validTimes.reduce((a, b) => (a > b ? a : b), -Infinity);
  const cut12mo = new Date(maxTime);
  cut12mo.setFullYear(cut12mo.getFullYear() - 1);

  return distFeatures.filter((f) => {
    const t = new Date((f.properties?.date as string) ?? "").getTime();
    return !isNaN(t) && t > cut12mo.getTime() && t <= maxTime;
  }).length;
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * Build the full district table rows from a raw GeoJSON features array.
 * Only includes districts with ≥ minTx transactions in the last 12 months.
 *
 * @param features - GeoJSON feature array (each with properties: district, city, unitPrice, date)
 * @param minTx    - Minimum transaction count gate (default 10)
 */
export function computeDistrictRows(features: any[], minTx = 10): DistrictRow[] {
  if (!features || features.length === 0) return [];

  // Collect unique (district, city) pairs
  const seen = new Map<string, { district: string; city: string }>();
  for (const f of features) {
    const d = f.properties?.district;
    const c = f.properties?.city ?? "";
    if (!d) continue;
    const key = `${c}::${d}`;
    if (!seen.has(key)) seen.set(key, { district: d, city: c });
  }

  const rows: DistrictRow[] = [];
  for (const { district, city } of seen.values()) {
    const txCount12mo = countTx12mo(features, district, city || undefined);
    if (txCount12mo < minTx) continue;

    rows.push({
      district,
      city,
      medianPriceWan: computeMedianPrice12mo(features, district, city || undefined),
      yoyPct: computeYoYPct(features, district, city || undefined),
      txCount12mo,
      buyerTimingScore: computeBuyerTimingScore(features, district, city || undefined),
      sparkline6mo: compute6MonthSparkline(features, district, city || undefined),
    });
  }

  return rows;
}

/**
 * Sort district rows by the given key and direction.
 * Null values always sort last (regardless of direction).
 */
export function sortRows(rows: DistrictRow[], key: SortKey, dir: SortDir): DistrictRow[] {
  return [...rows].sort((a, b) => {
    let av: string | number | null;
    let bv: string | number | null;

    switch (key) {
      case "district":
        av = `${a.city}${a.district}`;
        bv = `${b.city}${b.district}`;
        break;
      case "medianPrice":
        av = a.medianPriceWan;
        bv = b.medianPriceWan;
        break;
      case "yoyPct":
        av = a.yoyPct;
        bv = b.yoyPct;
        break;
      case "txCount":
        av = a.txCount12mo;
        bv = b.txCount12mo;
        break;
      case "timingScore":
        av = a.buyerTimingScore;
        bv = b.buyerTimingScore;
        break;
    }

    // Null always last
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;

    if (typeof av === "string" && typeof bv === "string") {
      return dir === "asc" ? av.localeCompare(bv, "zh-TW") : bv.localeCompare(av, "zh-TW");
    }
    const diff = (av as number) - (bv as number);
    return dir === "asc" ? diff : -diff;
  });
}

/** Returns CSS class and emoji for a buyer timing score. */
export function buyerScoreBucket(score: number): { label: string; cls: string; emoji: string } {
  if (score >= 70) return { label: "買方優勢", cls: "score-green",  emoji: "🟢" };
  if (score >= 40) return { label: "均衡市場", cls: "score-yellow", emoji: "🟡" };
  return               { label: "賣方市場", cls: "score-red",    emoji: "🔴" };
}
