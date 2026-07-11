/**
 * Pure utility functions for transit-proximity commute scoring (issue #171).
 * Phase 1: rail-station-proximity proxy using Haversine distances.
 * No DOM/window dependencies — fully testable with Vitest.
 *
 * Scoring model:
 *  - commuteScore(district → anchor): 0 = far, 100 = at the station
 *  - transitProximityScore(district → nearest MRT/TRA/HSR): 0–100
 *
 * The anchor is user-tunable (selected in the wizard Step 2).
 * Both MRT and TRA/HSR hubs are included so users outside MRT-served areas
 * can still set a meaningful anchor.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

/** Rail transit type for display. */
export type TransitType = "mrt" | "tra" | "hsr" | "bus";

/** A tunable anchor or transit station with coordinates. */
export interface TransitHub {
  id: string;
  label: string;          // display label (Chinese)
  lat: number;
  lng: number;
  type: TransitType;      // mrt | tra | hsr
  city?: string;          // optional city context
}

// ── Station / hub catalogue ────────────────────────────────────────────────────
// Covers Taiwan's 3 target cities: 台北市, 新北市, 桃園市.
// TRA = Taiwan Railways; HSR = Taiwan High Speed Rail; MRT = Metro (Taipei system).

/** Extended commute hub catalogue covering MRT, TRA, and HSR nodes. */
export const TRANSIT_HUBS: TransitHub[] = [
  // ── Major MRT interchange / terminus ──────────────────────────────────────
  { id: "taipei-main",   label: "台北車站 (MRT/TRA/HSR)", lat: 25.0478, lng: 121.5170, type: "mrt",  city: "台北市" },
  { id: "xinyi-anhe",    label: "信義安和站",              lat: 25.0330, lng: 121.5575, type: "mrt",  city: "台北市" },
  { id: "zhongshan",     label: "中山站",                  lat: 25.0525, lng: 121.5200, type: "mrt",  city: "台北市" },
  { id: "nanjing-sanmin",label: "南京三民站",              lat: 25.0519, lng: 121.5589, type: "mrt",  city: "台北市" },
  { id: "songshan",      label: "松山站 (MRT/TRA)",        lat: 25.0499, lng: 121.5780, type: "mrt",  city: "台北市" },
  { id: "nangang",       label: "南港站 (MRT/TRA/HSR)",    lat: 25.0524, lng: 121.6070, type: "mrt",  city: "台北市" },
  { id: "daan",          label: "大安站",                  lat: 25.0263, lng: 121.5433, type: "mrt",  city: "台北市" },
  { id: "zhongxiao-fuxing", label: "忠孝復興站",           lat: 25.0415, lng: 121.5468, type: "mrt",  city: "台北市" },
  { id: "xindian",       label: "新店站",                  lat: 24.9594, lng: 121.5345, type: "mrt",  city: "新北市" },
  { id: "banqiao",       label: "板橋站 (MRT/TRA/HSR)",    lat: 25.0142, lng: 121.4633, type: "hsr",  city: "新北市" },
  { id: "xinzhuang",     label: "新莊站",                  lat: 25.0365, lng: 121.4449, type: "mrt",  city: "新北市" },
  { id: "luzhou-mrt",    label: "蘆洲站 (MRT)",            lat: 25.0798, lng: 121.4664, type: "mrt",  city: "新北市" },
  { id: "tamsui",        label: "淡水站 (MRT)",            lat: 25.1676, lng: 121.4422, type: "mrt",  city: "新北市" },
  { id: "xinbei-tucheng",label: "土城站 (MRT)",            lat: 24.9726, lng: 121.4347, type: "mrt",  city: "新北市" },
  // ── TRA stations ───────────────────────────────────────────────────────────
  { id: "shulin-tra",    label: "樹林站 (TRA)",            lat: 24.9915, lng: 121.4235, type: "tra",  city: "新北市" },
  { id: "xizhi-tra",     label: "汐止站 (TRA)",            lat: 25.0676, lng: 121.6593, type: "tra",  city: "新北市" },
  { id: "yingge-tra",    label: "鶯歌站 (TRA)",            lat: 24.9627, lng: 121.3448, type: "tra",  city: "新北市" },
  { id: "sanxia-tra",    label: "三峽近鶯歌 (TRA)",       lat: 24.9441, lng: 121.3694, type: "tra",  city: "新北市" },
  { id: "ruifang-tra",   label: "瑞芳站 (TRA)",            lat: 25.1047, lng: 121.8025, type: "tra",  city: "新北市" },
  // ── Taoyuan TRA / MRT ───────────────────────────────────────────────────────
  { id: "taoyuan-main",  label: "桃園站 (TRA)",            lat: 24.9891, lng: 121.3139, type: "tra",  city: "桃園市" },
  { id: "zhongli-tra",   label: "中壢站 (TRA)",            lat: 24.9549, lng: 121.2245, type: "tra",  city: "桃園市" },
  { id: "yangmei-tra",   label: "楊梅站 (TRA)",            lat: 24.9140, lng: 121.1415, type: "tra",  city: "桃園市" },
  { id: "dasi-bus",      label: "大溪市中心 (客運)",      lat: 24.8785, lng: 121.2894, type: "bus",  city: "桃園市" },
  { id: "a18-mrt",       label: "機場捷運 A18 高鐵桃園", lat: 25.0099, lng: 121.2323, type: "mrt",  city: "桃園市" },
  // ── HSR ────────────────────────────────────────────────────────────────────
  { id: "hsr-taoyuan",   label: "高鐵桃園站",              lat: 24.9978, lng: 121.2333, type: "hsr",  city: "桃園市" },
];

// ── Geometry ──────────────────────────────────────────────────────────────────

/** Haversine great-circle distance in kilometres. */
export function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

// ── Scoring ───────────────────────────────────────────────────────────────────

/**
 * Compute a 0–100 commute score from a district centroid to an anchor hub.
 *
 * Decay model: exponential decay with half-life at 8 km (typical edge of
 * "convenient" transit zone). Score 100 at 0 km, ~50 at 8 km, ~25 at 16 km.
 *
 * @param districtLat - latitude of district centroid
 * @param districtLng - longitude of district centroid
 * @param anchor      - target anchor hub (user-chosen)
 */
export function computeAnchorCommuteScore(
  districtLat: number,
  districtLng: number,
  anchor: { lat: number; lng: number },
): number {
  const km = haversineKm(districtLat, districtLng, anchor.lat, anchor.lng);
  const HALF_LIFE_KM = 8;
  const raw = Math.pow(0.5, km / HALF_LIFE_KM);
  return Math.round(raw * 100);
}

/**
 * Find the nearest transit hub from a list to a district centroid.
 * Returns the hub and its distance in km.
 */
export function findNearestHub(
  districtLat: number,
  districtLng: number,
  hubs: TransitHub[],
): { hub: TransitHub; distanceKm: number } | null {
  if (hubs.length === 0) return null;

  let nearest = hubs[0]!;
  let minKm = haversineKm(districtLat, districtLng, nearest.lat, nearest.lng);

  for (let i = 1; i < hubs.length; i++) {
    const h = hubs[i]!;
    const d = haversineKm(districtLat, districtLng, h.lat, h.lng);
    if (d < minKm) { minKm = d; nearest = h; }
  }

  return { hub: nearest, distanceKm: minKm };
}

/**
 * Compute a 0–100 transit-proximity score for a district based on its distance
 * to the nearest transit station (any type in the hub list).
 *
 * Uses the same exponential decay as computeAnchorCommuteScore.
 */
export function computeTransitProximityScore(
  districtLat: number,
  districtLng: number,
  hubs: TransitHub[],
): { score: number; nearestHub: TransitHub | null; distanceKm: number } {
  const nearest = findNearestHub(districtLat, districtLng, hubs);
  if (!nearest) return { score: 0, nearestHub: null, distanceKm: Infinity };

  const score = computeAnchorCommuteScore(districtLat, districtLng, nearest.hub);
  return { score, nearestHub: nearest.hub, distanceKm: nearest.distanceKm };
}

// ── Label helpers ─────────────────────────────────────────────────────────────

/** Transit type emoji for display. */
export const TRANSIT_TYPE_EMOJI: Record<TransitType, string> = {
  mrt: "🚇",
  tra: "🚂",
  hsr: "🚄",
  bus: "🚌",
};

/**
 * Commute score label based on score threshold.
 * Used in UI badges and scorecard summaries.
 */
export function commuteScoreLabel(score: number): string {
  if (score >= 80) return "通勤便利";
  if (score >= 55) return "通勤尚可";
  if (score >= 30) return "需開車通勤";
  return "通勤偏遠";
}

/** One-liner commute badge, e.g. "🚄 通勤便利 (88) — 高鐵桃園站 2.1km". */
export function commuteBadge(
  score: number,
  anchorHub: TransitHub,
  distanceKm: number,
): string {
  const emoji = TRANSIT_TYPE_EMOJI[anchorHub.type];
  const label = commuteScoreLabel(score);
  const dist = distanceKm.toFixed(1);
  return `${emoji} ${label} (${score}) — ${anchorHub.label} ${dist} km`;
}

// ── Anchor select options ─────────────────────────────────────────────────────

/** Group hubs by city for grouped <select> options. */
export function groupHubsByCity(hubs: TransitHub[]): Record<string, TransitHub[]> {
  const result: Record<string, TransitHub[]> = {};
  for (const hub of hubs) {
    const city = hub.city ?? "其他";
    if (!result[city]) result[city] = [];
    result[city]!.push(hub);
  }
  return result;
}
