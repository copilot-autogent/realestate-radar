/**
 * Pure utility functions for the national timing summary banner (issue #156).
 * Computes a pre-district aggregate from the full loaded PLVR feature set —
 * zero new API calls, no DOM/window dependencies.
 *
 * IMPORTANT: Taiwan gov data uses full-width digits (U+FF10–FF19).
 * All date parsing normalises with .normalize('NFKC') before regex.
 */

export interface NationalTimingSummary {
  /** YoY % change: trailing 12mo aggregate median vs prior 12mo median. Null when < 24 months of data. */
  yoyPriceChange: number | null;
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
  /** false when fewer than 24 distinct (year, month) pairs are available */
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

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Compute a national timing summary from a GeoJSON feature array.
 *
 * @param features  Array of GeoJSON features with `properties.unitPrice`, `properties.date`
 * @returns NationalTimingSummary — sufficient = false when < 24 months of data
 */
export function computeNationalTimingSummary(features: any[]): NationalTimingSummary {
  const INSUFFICIENT: NationalTimingSummary = {
    yoyPriceChange: null,
    buyerAdvantageRatio: null,
    verdict: "yellow",
    verdictEmoji: "🟡",
    verdictLabel: "數據不足",
    verdictSummary: "歷史數據不足，請選擇行政區查看詳細分析",
    sufficient: false,
  };

  if (!Array.isArray(features) || features.length === 0) return INSUFFICIENT;

  // ── Step 1: Accumulate per-month unit prices and transaction counts ──────────
  const monthPrices = new Map<string, number[]>();
  const monthCounts = new Map<string, number>();
  let latestMonth = "";

  for (const f of features) {
    const p = f?.properties;
    if (!p) continue;
    const up = Number(p.unitPrice);
    if (!isFinite(up) || up <= 0) continue;
    const monthKey = parseDateMonth(p.date);
    if (!monthKey) continue;
    if (monthKey > latestMonth) latestMonth = monthKey;
    if (!monthPrices.has(monthKey)) monthPrices.set(monthKey, []);
    monthPrices.get(monthKey)!.push(up);
    monthCounts.set(monthKey, (monthCounts.get(monthKey) ?? 0) + 1);
  }

  // Require at least 24 distinct months — no point computing partial YoY
  if (!latestMonth || monthPrices.size < 24) return INSUFFICIENT;

  // ── Step 2: YoY price change — trailing 12mo vs prior 12mo ──────────────────
  // trailing window: [latestMonth - 11 .. latestMonth]
  // prior window:    [latestMonth - 23 .. latestMonth - 12]
  const trailing12Start = subtractMonths(latestMonth, 11);
  const prior12End      = subtractMonths(latestMonth, 12);
  const prior12Start    = subtractMonths(latestMonth, 23);

  const trailingPrices: number[] = [];
  const priorPrices: number[] = [];

  for (const [month, prices] of monthPrices) {
    if (month >= trailing12Start && month <= latestMonth) {
      trailingPrices.push(...prices);
    } else if (month >= prior12Start && month <= prior12End) {
      priorPrices.push(...prices);
    }
  }

  let yoyPriceChange: number | null = null;
  if (trailingPrices.length > 0 && priorPrices.length > 0) {
    const trailingMedian = median(trailingPrices);
    const priorMedian = median(priorPrices);
    if (priorMedian > 0) {
      yoyPriceChange = ((trailingMedian - priorMedian) / priorMedian) * 100;
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
  let verdict: "green" | "yellow" | "red" = "yellow";
  let verdictEmoji = "🟡";
  let verdictLabel = "市場觀望";
  let verdictSummary = "全台行情平穩，建議選擇行政區查看詳細分析";

  if (yoyPriceChange !== null && buyerAdvantageRatio !== null) {
    const priceFalling = yoyPriceChange <= -1;
    const priceRising  = yoyPriceChange >= 3;
    const buyerDom     = buyerAdvantageRatio >= 50;

    if (priceFalling || (Math.abs(yoyPriceChange) < 1 && buyerDom)) {
      verdict = "green";
      verdictEmoji = "🟢";
      verdictLabel = "買方有利";
      const pctStr =
        yoyPriceChange < -0.05
          ? `▼${Math.abs(yoyPriceChange).toFixed(1)}%`
          : "持平";
      verdictSummary = `近12個月成交量低於歷史高峰，議價空間擴大（單價 ${pctStr} YoY）`;
    } else if (priceRising && !buyerDom) {
      verdict = "red";
      verdictEmoji = "🔴";
      verdictLabel = "賣方主導";
      const pctStr = `▲${Math.abs(yoyPriceChange).toFixed(1)}%`;
      verdictSummary = `近12個月成交量維持高水位，賣方議價力強（單價 ${pctStr} YoY）`;
    } else {
      const dirStr =
        yoyPriceChange > 0.5
          ? `▲${Math.abs(yoyPriceChange).toFixed(1)}%`
          : yoyPriceChange < -0.5
          ? `▼${Math.abs(yoyPriceChange).toFixed(1)}%`
          : "持平";
      verdictSummary = `全台行情 ${dirStr} YoY，買方優勢月比例 ${buyerAdvantageRatio}%`;
    }
  } else {
    verdictSummary = "部分數據不足，建議選擇行政區查看詳細分析";
  }

  return {
    yoyPriceChange,
    buyerAdvantageRatio,
    verdict,
    verdictEmoji,
    verdictLabel,
    verdictSummary,
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
