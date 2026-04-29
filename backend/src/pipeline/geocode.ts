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
 * Strips floor suffix (e.g., "3樓"), partial unit identifiers.
 */
export function normalizeAddress(address: string): string {
  return address
    .replace(/\d+樓(之\d+)?$/u, "")   // remove floor suffix
    .replace(/[A-Z]棟/u, "")           // remove building wing
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
