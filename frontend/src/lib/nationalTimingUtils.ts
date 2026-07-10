/**
 * Pure utility functions for the national timing summary banner (issue #156).
 * Computes a pre-district aggregate from the full loaded PLVR feature set —
 * zero new API calls, no DOM/window dependencies.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 *
 * Data sufficiency tiers (issue #165):
 *   Tier A — ≥24 months: full YoY comparison (original behaviour)
 *   Tier B — 12–23 months: short-term 6mo trend + buyer-advantage ratio
 *   Tier C — <12 months: insufficient → 數據不足 wall
 */

export interface NationalTimingSummary {
  /** YoY % change: trailing 12mo aggregate median vs prior 12mo median. Null when < 24 months of data. */
  yoyPriceChange: number | null;
  /**
   * Short-term price change: trailing 6mo median vs prior 6mo median (%).
   * Available when ≥ 12 months of priced data are present (Tier A and B).
   */
  shortTermPriceChange: number | null;
  /**
   * % of last 12 calendar months that are buyer-advantage months (0–100).
   * Denominator is the number of months that had ≥1 transaction (min 6 to qualify).
   * Null when insufficient.
   */
  buyerAdvantageRatio: number | null;
  /** Composite verdict */
  verdict: "green" | "yellow" | "red";
  verdictEmoji: string;
  verdictLabel: string;
  /** One-line summary shown in the banner */
  verdictSummary: string;
  /**
   * Data sufficiency tier:
   *   "A" — full YoY window (≥24 months)
   *   "B" — partial data (12–23 months); short-term signals available
   *   "C" — insufficient (<12 months); nothing useful to show
   */
  tier: "A" | "B" | "C";
  /** Number of distinct (YYYY-MM) months in the dataset with priced transactions. */
  dataMonths: number;
  /**
   * Qualifier string appended to the banner title when data is partial.
   * "(近期資料)" for Tier B, empty string for Tier A and C.
   */
  dataQualifier: string;
  /**
   * National composite buyer-timing score (0–100), computed from price direction
   * and buyer-advantage ratio.  Null only when tier is "C".
   */
  nationalBuyerScore: number | null;
  /**
   * true when tier is "A" or "B" (≥12 months of usable data).
   * @deprecated Prefer `tier !== "C"` for clarity; kept for backward compatibility.
   */
  sufficient: boolean;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function parseDateMonth(raw: unknown): string | null {
  if (!raw || typeof raw !== "string") return null;
  const s = raw.normalize("NFKC");
  // Accept trailing: -, /, space, T (ISO-8601), or end-of-string
  const m = /^(\d{4})[-/](\d{1,2})(?:[-/\sT]|$)/.exec(s);
  if (!m) return null;
  const year = parseInt(m[1]!, 10);
  const month = parseInt(m[2]!, 10);
  if (isNaN(year) || isNaN(month) || year < 1900 || year > 2100 || month < 1 || month > 12) {
    return null;
  }
  return `${m[1]!}-${String(month).padStart(2, "0")}`;
}

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[mid - 1]! + s[mid]!) / 2 : s[mid]!;
}

/** Returns the "YYYY-MM" that is exactly `n` months before `anchorMonth`. */
export function subtractMonths(anchorMonth: string, n: number): string {
  let year = parseInt(anchorMonth.slice(0, 4), 10);
  let month = parseInt(anchorMonth.slice(5, 7), 10);
  month -= n;
  while (month <= 0) {
    month += 12;
    year -= 1;
  }
  return `${year}-${String(month).padStart(2, "0")}`;
}

// ── Internal score helper ─────────────────────────────────────────────────────

/** Maps a price-change percentage to a buyer-friendliness score (0–100).
 *  pct ≤ -20% → 100 (strongly buyer-friendly), pct ≥ +20% → 0 (seller-friendly). */
function pctToScore(pct: number): number {
  return Math.max(0, Math.min(100, (-pct + 20) / 40 * 100));
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Compute a national timing summary from a GeoJSON feature array.
 *
 * Implements a tiered fallback (issue #165):
 *   Tier A (≥24 months): full 12mo-vs-12mo YoY comparison — original behaviour
 *   Tier B (12–23 months): short-term 6mo trend + buyer-advantage ratio
 *   Tier C (<12 months): 數據不足 — no useful signals available
 *
 * @param features  Array of GeoJSON features with `properties.unitPrice`, `properties.date`
 */
export function computeNationalTimingSummary(features: any[]): NationalTimingSummary {
  const tierC = (dataMonths = 0): NationalTimingSummary => ({
    yoyPriceChange: null,
    shortTermPriceChange: null,
    buyerAdvantageRatio: null,
    verdict: "yellow",
    verdictEmoji: "🟡",
    verdictLabel: "數據不足",
    verdictSummary: "歷史數據不足（< 12個月），請選擇行政區查看詳細分析",
    tier: "C",
    dataMonths,
    dataQualifier: "",
    nationalBuyerScore: null,
    sufficient: false,
  });

  if (!Array.isArray(features) || features.length === 0) return tierC();

  // ── Step 1: Accumulate per-month unit prices and transaction counts ──────────
  // monthCounts uses ALL transactions (not just priced) for volume signal accuracy
  const monthPrices = new Map<string, number[]>();
  const monthCounts = new Map<string, number>();
  let latestMonth = "";

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;
    const monthKey = parseDateMonth(p.date);
    if (!monthKey) continue;
    if (monthKey > latestMonth) latestMonth = monthKey;
    // Count all transactions (volume signal)
    monthCounts.set(monthKey, (monthCounts.get(monthKey) ?? 0) + 1);
    // Prices only for priced transactions
    const up = Number(p.unitPrice);
    if (isFinite(up) && up > 0) {
      if (!monthPrices.has(monthKey)) monthPrices.set(monthKey, []);
      monthPrices.get(monthKey)!.push(up);
    }
  }

  const dataMonths = monthPrices.size;
  if (!latestMonth || dataMonths < 12) return tierC(dataMonths);

  const tier: "A" | "B" = dataMonths >= 24 ? "A" : "B";

  // ── Step 2a: Short-term price change — trailing 6mo vs prior 6mo ─────────────
  // Always computed when ≥12 months available (used for Tier B verdict and score).
  const trailing6Start = subtractMonths(latestMonth, 5);
  const prior6End      = subtractMonths(latestMonth, 6);
  const prior6Start    = subtractMonths(latestMonth, 11);

  const trailing6Prices: number[] = [];
  const prior6Prices: number[] = [];

  for (const [month, prices] of monthPrices) {
    if (month >= trailing6Start && month <= latestMonth) {
      trailing6Prices.push(...prices);
    } else if (month >= prior6Start && month <= prior6End) {
      prior6Prices.push(...prices);
    }
  }

  let shortTermPriceChange: number | null = null;
  if (trailing6Prices.length > 0 && prior6Prices.length > 0) {
    const t6Median = median(trailing6Prices);
    const p6Median = median(prior6Prices);
    if (p6Median > 0) {
      shortTermPriceChange = ((t6Median - p6Median) / p6Median) * 100;
    }
  }

  // ── Step 2b: Full YoY price change — trailing 12mo vs prior 12mo (Tier A only) ─
  const trailing12Start = subtractMonths(latestMonth, 11);
  const prior12End      = subtractMonths(latestMonth, 12);
  const prior12Start    = subtractMonths(latestMonth, 23);

  let yoyPriceChange: number | null = null;
  if (tier === "A") {
    const trailingPrices: number[] = [];
    const priorPrices: number[] = [];

    for (const [month, prices] of monthPrices) {
      if (month >= trailing12Start && month <= latestMonth) {
        trailingPrices.push(...prices);
      } else if (month >= prior12Start && month <= prior12End) {
        priorPrices.push(...prices);
      }
    }

    if (trailingPrices.length > 0 && priorPrices.length > 0) {
      const trailingMedian = median(trailingPrices);
      const priorMedian = median(priorPrices);
      if (priorMedian > 0) {
        yoyPriceChange = ((trailingMedian - priorMedian) / priorMedian) * 100;
      }
    }
  }

  // ── Step 3: Buyer-advantage month ratio for last 12 months ──────────────────
  // A month is "buyer-advantaged" when its count < 70% of that year's monthly peak
  // (mirrors the heatmap definition in heatmapUtils.ts).
  // Denominator: months with ≥1 transaction (min 6 required to emit a ratio).
  const yearPeaks = new Map<number, number>();
  for (const [month, count] of monthCounts) {
    const year = parseInt(month.slice(0, 4), 10);
    yearPeaks.set(year, Math.max(yearPeaks.get(year) ?? 0, count));
  }

  // Build ordered list of last 12 months ending at latestMonth
  const last12Months: string[] = [];
  {
    let y = parseInt(latestMonth.slice(0, 4), 10);
    let mo = parseInt(latestMonth.slice(5, 7), 10);
    for (let i = 0; i < 12; i++) {
      last12Months.unshift(`${y}-${String(mo).padStart(2, "0")}`);
      mo--;
      if (mo < 1) { mo = 12; y--; }
    }
  }

  let buyerAdvMonths = 0;
  let presentMonths = 0;
  for (const m of last12Months) {
    const count = monthCounts.get(m) ?? 0;
    const year = parseInt(m.slice(0, 4), 10);
    const peak = yearPeaks.get(year) ?? 0;
    if (count > 0 && peak > 0) {
      presentMonths++;
      if (count < peak * 0.7) buyerAdvMonths++;
    }
  }

  // Divide by presentMonths (not a fixed 12) so sparse months don't inflate/deflate ratio
  const buyerAdvantageRatio: number | null =
    presentMonths >= 6 ? Math.round((buyerAdvMonths / presentMonths) * 100) : null;

  // ── Step 4: Composite verdict ────────────────────────────────────────────────
  // Tier A: uses full YoY price change + buyer-advantage ratio
  // Tier B: uses short-term 6mo price change + buyer-advantage ratio
  let verdict: "green" | "yellow" | "red" = "yellow";
  let verdictEmoji = "🟡";
  let verdictLabel = "市場觀望";
  let verdictSummary: string;

  const primaryPriceChange = tier === "A" ? yoyPriceChange : shortTermPriceChange;
  const buyerDom = buyerAdvantageRatio !== null && buyerAdvantageRatio >= 50;

  if (primaryPriceChange !== null && buyerAdvantageRatio !== null) {
    const priceFalling = primaryPriceChange <= -1;
    const priceRising  = primaryPriceChange >= 3;

    if (tier === "A") {
      // Tier A verdict logic (original behaviour)
      const yoyClause =
        Math.abs(primaryPriceChange) < 0.05
          ? "單價持平"
          : primaryPriceChange < 0
          ? `單價 ▼${Math.abs(primaryPriceChange).toFixed(1)}% YoY`
          : `單價 ▲${Math.abs(primaryPriceChange).toFixed(1)}% YoY`;

      if (priceFalling || (Math.abs(primaryPriceChange) < 1 && buyerDom)) {
        verdict = "green"; verdictEmoji = "🟢"; verdictLabel = "買方有利";
        verdictSummary = `近12個月成交量低於歷史高峰，議價空間擴大（${yoyClause}）`;
      } else if (priceRising && !buyerDom) {
        verdict = "red"; verdictEmoji = "🔴"; verdictLabel = "賣方主導";
        verdictSummary = `近12個月成交量維持高水位，賣方議價力強（${yoyClause}）`;
      } else {
        verdictSummary = `全台行情 ${formatYoY(yoyPriceChange)} YoY，買方優勢月比例 ${buyerAdvantageRatio}%`;
      }
    } else {
      // Tier B verdict logic — based on 6mo short-term trend
      const stClause =
        Math.abs(primaryPriceChange) < 0.05
          ? "近6個月均價持平"
          : primaryPriceChange < 0
          ? `近6個月均價 ▼${Math.abs(primaryPriceChange).toFixed(1)}%`
          : `近6個月均價 ▲${Math.abs(primaryPriceChange).toFixed(1)}%`;

      if (priceFalling || (Math.abs(primaryPriceChange) < 1 && buyerDom)) {
        verdict = "green"; verdictEmoji = "🟢"; verdictLabel = "買方有利";
        verdictSummary = `${stClause}，買方優勢月比例 ${buyerAdvantageRatio}%`;
      } else if (priceRising && !buyerDom) {
        verdict = "red"; verdictEmoji = "🔴"; verdictLabel = "賣方主導";
        verdictSummary = `${stClause}，成交量維持高水位`;
      } else {
        verdictSummary = `${stClause}，買方優勢月比例 ${buyerAdvantageRatio}%`;
      }
    }
  } else if (primaryPriceChange !== null) {
    verdictSummary = tier === "A"
      ? `全台行情 ${formatYoY(yoyPriceChange)} YoY`
      : `近6個月均價 ${formatYoY(shortTermPriceChange)}`;
  } else if (buyerAdvantageRatio !== null) {
    verdictSummary = `買方優勢月比例 ${buyerAdvantageRatio}%，建議選擇行政區查看詳細分析`;
  } else {
    verdictSummary = "部分數據不足，建議選擇行政區查看詳細分析";
  }

  // ── Step 5: National buyer timing score ──────────────────────────────────────
  // Composite of price-direction signal (50%) + buyer-advantage ratio (50%).
  // Uses best available price signal: full YoY for Tier A, 6mo trend for Tier B.
  let nationalBuyerScore: number | null = null;
  if (buyerAdvantageRatio !== null) {
    const priceForScore = yoyPriceChange ?? shortTermPriceChange;
    const priceSignal = priceForScore !== null ? pctToScore(priceForScore) : 50;
    nationalBuyerScore = Math.round(0.5 * priceSignal + 0.5 * buyerAdvantageRatio);
  }

  const dataQualifier = tier === "B" ? "（近期資料）" : "";

  return {
    yoyPriceChange,
    shortTermPriceChange,
    buyerAdvantageRatio,
    verdict,
    verdictEmoji,
    verdictLabel,
    verdictSummary,
    tier,
    dataMonths,
    dataQualifier,
    nationalBuyerScore,
    sufficient: true,
  };
}

/**
 * Format the YoY price change as a display string (e.g. "▼2.8%", "▲1.2%", "持平").
 * Returns "—" when yoyPriceChange is null.
 */
export function formatYoY(yoy: number | null): string {
  if (yoy === null) return "—";
  if (Math.abs(yoy) < 0.05) return "持平";
  const abs = Math.abs(yoy).toFixed(1);
  return yoy < 0 ? `▼${abs}%` : `▲${abs}%`;
}
