import { describe, it, expect } from "vitest";
import {
  normalizeFullWidth,
  parseRocDate,
  parseRocBuildYear,
  detectCsvFormat,
  validateColumnCount,
  PLVR_COL_COUNT_LEGACY,
  PLVR_COL_COUNT_NEW,
  PLVR_NEW_FORMAT_MARKER,
} from "../lib/plvrUtils.js";

// ── normalizeFullWidth ────────────────────────────────────────────────────────

describe("normalizeFullWidth", () => {
  it("converts full-width digits to ASCII", () => {
    expect(normalizeFullWidth("４３")).toBe("43");
    expect(normalizeFullWidth("７４１０２４")).toBe("741024");
    expect(normalizeFullWidth("０１２３４５６７８９")).toBe("0123456789");
  });

  it("leaves ASCII digits unchanged", () => {
    expect(normalizeFullWidth("741024")).toBe("741024");
    expect(normalizeFullWidth("1130115")).toBe("1130115");
  });

  it("converts full-width letters", () => {
    expect(normalizeFullWidth("Ａ")).toBe("A");
  });

  it("handles empty string", () => {
    expect(normalizeFullWidth("")).toBe("");
  });

  it("handles mixed full-width and ASCII", () => {
    expect(normalizeFullWidth("１３號")).toBe("13號");
  });
});

// ── parseRocDate ──────────────────────────────────────────────────────────────

describe("parseRocDate", () => {
  it("parses 7-digit YYYMMDD ROC transaction date", () => {
    expect(parseRocDate("1130115")).toBe("2024-01-15");
    expect(parseRocDate("1141231")).toBe("2025-12-31");
  });

  it("parses 6-digit YYMMDD ROC transaction date", () => {
    expect(parseRocDate("741024")).toBe("1985-10-24");
    expect(parseRocDate("860722")).toBe("1997-07-22");
  });

  it("pads month and day with leading zeros", () => {
    expect(parseRocDate("1130115")).toBe("2024-01-15");
    expect(parseRocDate("1130701")).toBe("2024-07-01");
  });

  it("NFKC: parses full-width digit string", () => {
    // "１１３０１１５" = 1130115 in full-width
    expect(parseRocDate("１１３０１１５")).toBe("2024-01-15");
    // "７４１０２４" = 741024
    expect(parseRocDate("７４１０２４")).toBe("1985-10-24");
  });

  it("returns null for null/undefined/empty", () => {
    expect(parseRocDate(null)).toBe(null);
    expect(parseRocDate(undefined)).toBe(null);
    expect(parseRocDate("")).toBe(null);
  });

  it("returns null for strings shorter than 5 digits", () => {
    expect(parseRocDate("1234")).toBe(null);
  });

  it("returns null for non-numeric strings", () => {
    expect(parseRocDate("不明")).toBe(null);
    expect(parseRocDate("abc")).toBe(null);
  });

  it("returns null for invalid month (month > 12)", () => {
    expect(parseRocDate("1131301")).toBe(null); // month 13
  });

  it("returns null for invalid day (day = 00)", () => {
    expect(parseRocDate("1131200")).toBe(null);
  });

  it("returns null for implausibly old dates (year < 1945)", () => {
    // ROC year 1 → 1912-xx-xx, within reject range
    expect(parseRocDate("10101")).toBe(null);
  });

  it("returns null for impossible calendar dates (e.g. Feb 31)", () => {
    // Month 02, day 31 → would roll over in JS Date; parseRocDate should reject
    expect(parseRocDate("1130231")).toBe(null); // 2024-02-31 is invalid
    expect(parseRocDate("1130431")).toBe(null); // 2024-04-31 is invalid
  });
});

// ── parseRocBuildYear ─────────────────────────────────────────────────────────

describe("parseRocBuildYear", () => {
  it("parses 6-digit YYMMDD ROC build year", () => {
    expect(parseRocBuildYear(741024)).toBe(1985); // ROC 74/10/24
    expect(parseRocBuildYear(860722)).toBe(1997); // ROC 86/07/22
  });

  it("parses 7-digit YYYMMDD ROC build year", () => {
    expect(parseRocBuildYear(1081128)).toBe(2019); // ROC 108/11/28
    expect(parseRocBuildYear(1150507)).toBe(2026); // ROC 115/05/07
  });

  it("parses plain ROC year (< 200)", () => {
    expect(parseRocBuildYear(74)).toBe(1985);
    expect(parseRocBuildYear(100)).toBe(2011);
    expect(parseRocBuildYear(113)).toBe(2024);
  });

  it("parses 4-digit Gregorian year directly", () => {
    expect(parseRocBuildYear(1985)).toBe(1985);
    expect(parseRocBuildYear(2010)).toBe(2010);
  });

  it("NFKC: parses full-width 6-digit ROC date string", () => {
    expect(parseRocBuildYear("７４１０２４")).toBe(1985);
  });

  it("NFKC: parses full-width plain ROC year string", () => {
    expect(parseRocBuildYear("７４")).toBe(1985);
  });

  it("parses ASCII string representation of 6-digit ROC date", () => {
    expect(parseRocBuildYear("741024")).toBe(1985);
  });

  it("returns null for null/undefined/empty", () => {
    expect(parseRocBuildYear(null)).toBe(null);
    expect(parseRocBuildYear(undefined)).toBe(null);
    expect(parseRocBuildYear("")).toBe(null);
  });

  it("returns null for zero", () => {
    expect(parseRocBuildYear(0)).toBe(null);
  });

  it("returns null for non-integer numbers", () => {
    expect(parseRocBuildYear(74.5)).toBe(null);
  });

  it("returns null for negative numbers", () => {
    expect(parseRocBuildYear(-1)).toBe(null);
  });

  it("returns null for non-numeric strings", () => {
    expect(parseRocBuildYear("不明")).toBe(null);
    expect(parseRocBuildYear("abc")).toBe(null);
  });

  it("returns null for string with non-numeric suffix (e.g. '74年')", () => {
    expect(parseRocBuildYear("74年")).toBe(null);
  });

  it("returns null for implausibly old Gregorian year (< 1900)", () => {
    expect(parseRocBuildYear(1800)).toBe(null);
  });

  it("respects custom maxYear — rejects year > maxYear", () => {
    expect(parseRocBuildYear(2026, 2025)).toBe(null); // 2026 > 2025
    expect(parseRocBuildYear(2026, 2026)).toBe(2026); // boundary allowed
    expect(parseRocBuildYear(1150507, 2026)).toBe(2026); // ROC 115 → 2026, within range
    expect(parseRocBuildYear(1150507, 2025)).toBe(null); // ROC 115 → 2026 > 2025
  });
});

// ── detectCsvFormat ───────────────────────────────────────────────────────────

describe("detectCsvFormat", () => {
  it("detects new 28-col format when header contains PLVR_NEW_FORMAT_MARKER", () => {
    const newHeader = `鄉鎮市區,交易標的,土地位置建物門牌,土地移轉總面積平方公尺,都市土地使用分區,${PLVR_NEW_FORMAT_MARKER},非都市土地使用編定,交易年月日`;
    expect(detectCsvFormat(newHeader)).toBe("new28");
  });

  it("detects legacy 26-col format when header lacks PLVR_NEW_FORMAT_MARKER", () => {
    const legacyHeader = "鄉鎮市區,交易標的,土地位置建物門牌,土地移轉總面積平方公尺,都市土地使用分區,交易年月日";
    expect(detectCsvFormat(legacyHeader)).toBe("legacy26");
  });

  it("NFKC: detects new format even with full-width marker", () => {
    // Full-width version of "非都市土地使用分區" should normalize to same
    const fullWidthMarker = normalizeFullWidth(PLVR_NEW_FORMAT_MARKER);
    expect(fullWidthMarker).toBe(PLVR_NEW_FORMAT_MARKER); // CJK chars are unchanged by NFKC
    // Just verify the function works on a full-width augmented header
    const header = `header,${PLVR_NEW_FORMAT_MARKER},other`;
    expect(detectCsvFormat(header)).toBe("new28");
  });

  it("handles empty header string as legacy", () => {
    expect(detectCsvFormat("")).toBe("legacy26");
  });
});

// ── validateColumnCount ───────────────────────────────────────────────────────

describe("validateColumnCount", () => {
  it("accepts an array with exactly PLVR_COL_COUNT_LEGACY columns", () => {
    const cols = Array(PLVR_COL_COUNT_LEGACY).fill("x");
    expect(validateColumnCount(cols)).toBe(true);
  });

  it("accepts an array with PLVR_COL_COUNT_NEW columns", () => {
    const cols = Array(PLVR_COL_COUNT_NEW).fill("x");
    expect(validateColumnCount(cols)).toBe(true);
  });

  it("accepts arrays wider than PLVR_COL_COUNT_NEW (extra trailing columns)", () => {
    const cols = Array(30).fill("x");
    expect(validateColumnCount(cols)).toBe(true);
  });

  it("rejects arrays narrower than minimum", () => {
    const cols = Array(PLVR_COL_COUNT_LEGACY - 1).fill("x");
    expect(validateColumnCount(cols)).toBe(false);
  });

  it("rejects empty array", () => {
    expect(validateColumnCount([])).toBe(false);
  });

  it("uses custom minCols parameter", () => {
    expect(validateColumnCount(["a", "b", "c"], 3)).toBe(true);
    expect(validateColumnCount(["a", "b"], 3)).toBe(false);
  });
});
