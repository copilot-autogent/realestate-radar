/**
 * Geocoding pipeline using Nominatim (OpenStreetMap)
 * Converts Taiwan addresses → lat/lon
 *
 * Rate limit: 1 req/sec (Nominatim ToS)
 * Caching: persistent disk cache at data/geocode-cache.json
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";

const NOMINATIM_URL = "https://nominatim.openstreetmap.org/search";
const CACHE_FILE = path.resolve(import.meta.dirname, "../../../data/geocode-cache.json");
const REQUEST_INTERVAL_MS = 1100; // slightly over 1s for Nominatim ToS compliance

export interface GeocodeResult {
  lat: number;
  lon: number;
  displayName: string;
}

type CacheEntry = GeocodeResult | null;
let cache: Record<string, CacheEntry> = {};
let cacheLoaded = false;
let pendingWrites = 0;

function loadCache(): void {
  if (cacheLoaded) return;
  if (existsSync(CACHE_FILE)) {
    try {
      cache = JSON.parse(readFileSync(CACHE_FILE, "utf-8")) as Record<string, CacheEntry>;
    } catch {
      cache = {};
    }
  }
  cacheLoaded = true;
}

function saveCache(): void {
  const dir = path.dirname(CACHE_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), "utf-8");
  pendingWrites = 0;
}

// Save cache periodically (every 20 writes) to avoid excessive I/O
function maybeSaveCache(): void {
  pendingWrites++;
  if (pendingWrites >= 20) saveCache();
}

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < REQUEST_INTERVAL_MS) {
    await new Promise(resolve => setTimeout(resolve, REQUEST_INTERVAL_MS - elapsed));
  }
  lastRequestTime = Date.now();
  return fetch(url, {
    headers: {
      "User-Agent": "RealPriceRadar/1.0 (https://github.com/copilot-autogent/realestate-radar)",
      "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    },
  });
}

/**
 * Normalize a Taiwan address for better geocoding results.
 * Nominatim resolves street-level queries but not specific house numbers.
 *
 * 實價登錄 data uses full-width Unicode digits (０-９) for house numbers
 * and floor numbers, so the regex must match both ASCII (0-9) and
 * full-width (０-９, U+FF10–U+FF19) digits.
 * Stripping the house number maximizes Nominatim street-level match rate.
 */
export function normalizeAddress(address: string): string {
  const D = "[\\d０-９]"; // ASCII or full-width digit (U+FF10–U+FF19)
  return address
    // Strip house number and everything after (floor, unit, ownership notes, etc.)
    // e.g. "新生北路三段４３號五樓之２６" → "新生北路三段"
    // e.g. "中山北路六段７５４巷１２號地下二層" → "中山北路六段７５４巷"
    //      (巷/lane number preserved; 號/house number stripped)
    .replace(new RegExp(`${D}+之?${D}*號.*`, "u"), "")
    // Fallback: strip trailing floor suffix when no 號 is present
    // e.g. "木柵路一段3樓" → "木柵路一段"
    .replace(new RegExp(`${D}+樓(之${D}+)?$`, "u"), "")
    // Remove building wing designator — case-insensitive handles a/A/ａ/Ａ
    .replace(/[A-Za-zＡ-Ｚａ-ｚ]棟/u, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Geocode a Taiwan address using Nominatim.
 * Returns lat/lon or null if not found.
 * Results are cached to disk.
 */
export async function geocodeAddress(address: string): Promise<GeocodeResult | null> {
  loadCache();

  const key = address.trim();
  if (key in cache) return cache[key];

  const normalized = normalizeAddress(key);
  const query = normalized.includes("台灣") || normalized.includes("Taiwan")
    ? normalized
    : `${normalized}, 台灣`;

  const params = new URLSearchParams({
    q: query,
    format: "json",
    limit: "1",
    countrycodes: "tw",
    addressdetails: "0",
  });

  try {
    const res = await rateLimitedFetch(`${NOMINATIM_URL}?${params.toString()}`);
    if (!res.ok) {
      console.warn(`[geocode] HTTP ${res.status} for "${address}"`);
      cache[key] = null;
      maybeSaveCache();
      return null;
    }

    const data = (await res.json()) as Array<{ lat: string; lon: string; display_name: string }>;
    if (!data || data.length === 0) {
      cache[key] = null;
      maybeSaveCache();
      return null;
    }

    const result: GeocodeResult = {
      lat: parseFloat(data[0].lat),
      lon: parseFloat(data[0].lon),
      displayName: data[0].display_name,
    };

    cache[key] = result;
    maybeSaveCache();
    return result;
  } catch (err) {
    console.warn(`[geocode] Error for "${address}":`, (err as Error).message);
    // Don't cache errors — allow retry next run
    return null;
  }
}

/** Flush pending cache writes to disk. Call before process exit. */
export function flushCache(): void {
  if (pendingWrites > 0 || !cacheLoaded) return;
  saveCache();
}

/** Number of entries currently in the cache. */
export function cacheSize(): number {
  loadCache();
  return Object.keys(cache).length;
}
