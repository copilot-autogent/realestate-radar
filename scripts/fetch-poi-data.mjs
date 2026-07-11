#!/usr/bin/env node
/**
 * fetch-poi-data.mjs
 *
 * Fetches per-district POI counts from OpenStreetMap (Overpass API) for the
 * three target cities (台北市, 新北市, 桃園市) and writes a static JSON file
 * to frontend/public/data/poi-scores.json.
 *
 * Usage:
 *   node scripts/fetch-poi-data.mjs [--dry-run]
 *
 * Output schema (poi-scores.json):
 *   { "<city>::<district>": { restaurant, sport, cinema, supermarket,
 *                             convenience, park, hospital } }
 *
 * This script is meant to run at build time or on-demand by a maintainer.
 * The generated JSON is committed to the repo — the static site has NO
 * runtime Overpass dependency.
 *
 * Gotchas:
 *  - NFKC-normalize district names when comparing with OSM data.
 *  - Rate-limit Overpass: 1 query per district with a 1-second delay between requests.
 *  - Overpass endpoint: https://overpass-api.de/api/interpreter
 *  - Queries use bounding boxes derived from the district GeoJSON centroids ± radius.
 */

import { readFileSync, writeFileSync } from "fs";
import { fileURLToPath } from "url";
import { join, dirname } from "path";
import https from "https";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUTPUT_PATH = join(ROOT, "frontend/public/data/poi-scores.json");
const DRY_RUN = process.argv.includes("--dry-run");

const OVERPASS_URL = "https://overpass-api.de/api/interpreter";
const REQUEST_DELAY_MS = 1200; // be polite to the public API
const BBOX_RADIUS_DEG = 0.025; // ~2.5 km half-width per district bounding box

// ── GeoJSON district files ───────────────────────────────────────────────────

const GEOJSON_FILES = [
  join(ROOT, "frontend/public/data/taipei-districts.geojson"),
  join(ROOT, "frontend/public/data/xinbei-districts.geojson"),
  join(ROOT, "frontend/public/data/taoyuan-districts.geojson"),
];

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Compute arithmetic centroid of a GeoJSON Polygon or MultiPolygon. */
function centroidOf(geometry) {
  let rings = [];
  if (geometry.type === "Polygon") {
    rings = geometry.coordinates;
  } else if (geometry.type === "MultiPolygon") {
    geometry.coordinates.forEach(poly => rings.push(...poly));
  }
  if (!rings.length) return null;
  const outer = rings[0];
  let sumLng = 0, sumLat = 0, n = 0;
  for (const [lng, lat] of outer) {
    sumLng += lng; sumLat += lat; n++;
  }
  return n ? { lat: sumLat / n, lng: sumLng / n } : null;
}

/** NFKC-normalize a string (handles full-width CJK digits). */
function nfkc(str) {
  return str.normalize("NFKC");
}

/** Sleep for ms milliseconds. */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** HTTP POST to Overpass API with the given QL body. Returns parsed JSON. */
function overpassQuery(ql) {
  return new Promise((resolve, reject) => {
    const body = `data=${encodeURIComponent(ql)}`;
    const opts = {
      hostname: "overpass-api.de",
      path: "/api/interpreter",
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": Buffer.byteLength(body),
        "User-Agent": "realestate-radar-build/1.0 (github.com/copilot-autogent/realestate-radar)",
      },
    };
    const req = https.request(opts, (res) => {
      let data = "";
      res.on("data", chunk => { data += chunk; });
      res.on("end", () => {
        if (res.statusCode !== 200) {
          reject(new Error(`Overpass HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`Overpass JSON parse error: ${e.message}`));
        }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

/**
 * Build an Overpass QL query that counts POIs in a bounding box.
 * Returns element counts per category tag.
 *
 * bbox = [south, west, north, east] (Overpass convention)
 */
function buildCountQuery(bbox) {
  const [s, w, n, e] = bbox;
  const b = `${s},${w},${n},${e}`;
  return `
[out:json][timeout:30];
(
  // 餐廳 (restaurants & cafes)
  node["amenity"~"^(restaurant|cafe|fast_food)$"](${b});
  way["amenity"~"^(restaurant|cafe|fast_food)$"](${b});
  // 運動中心/健身房
  node["leisure"~"^(sports_centre|fitness_centre|gym)$"](${b});
  way["leisure"~"^(sports_centre|fitness_centre|gym)$"](${b});
  node["amenity"="gym"](${b});
  // 電影院
  node["amenity"="cinema"](${b});
  way["amenity"="cinema"](${b});
  // 超市
  node["shop"~"^(supermarket|department_store)$"](${b});
  way["shop"~"^(supermarket|department_store)$"](${b});
  // 超商 (convenience stores)
  node["shop"="convenience"](${b});
  way["shop"="convenience"](${b});
  // 公園
  node["leisure"="park"](${b});
  way["leisure"="park"](${b});
  // 醫院/診所
  node["amenity"~"^(hospital|clinic|doctors|pharmacy)$"](${b});
  way["amenity"~"^(hospital|clinic|doctors|pharmacy)$"](${b});
);
out count;
  `.trim();
}

/**
 * Count-only query per category.
 * Returns an object: { restaurant, sport, cinema, supermarket, convenience, park, hospital }
 */
async function fetchPoiCounts(centroid) {
  const { lat, lng } = centroid;
  const bbox = [
    lat - BBOX_RADIUS_DEG,
    lng - BBOX_RADIUS_DEG,
    lat + BBOX_RADIUS_DEG,
    lng + BBOX_RADIUS_DEG,
  ];
  const b = bbox.join(",");

  // Run per-category queries
  const categories = {
    restaurant: `node["amenity"~"^(restaurant|cafe|fast_food)$"](${b}); way["amenity"~"^(restaurant|cafe|fast_food)$"](${b});`,
    sport:      `node["leisure"~"^(sports_centre|fitness_centre|gym)$"](${b}); way["leisure"~"^(sports_centre|fitness_centre|gym)$"](${b}); node["amenity"="gym"](${b});`,
    cinema:     `node["amenity"="cinema"](${b}); way["amenity"="cinema"](${b});`,
    supermarket:`node["shop"~"^(supermarket|department_store)$"](${b}); way["shop"~"^(supermarket|department_store)$"](${b});`,
    convenience:`node["shop"="convenience"](${b}); way["shop"="convenience"](${b});`,
    park:       `node["leisure"="park"](${b}); way["leisure"="park"](${b}); relation["leisure"="park"](${b});`,
    hospital:   `node["amenity"~"^(hospital|clinic|doctors|pharmacy)$"](${b}); way["amenity"~"^(hospital|clinic|doctors|pharmacy)$"](${b});`,
  };

  const results = {};
  for (const [cat, ql] of Object.entries(categories)) {
    const query = `[out:json][timeout:25];\n(\n${ql}\n);\nout count;`;
    try {
      const data = await overpassQuery(query);
      // Overpass "out count" returns elements[0].tags.total
      const total = parseInt(data?.elements?.[0]?.tags?.total ?? "0", 10);
      results[cat] = isNaN(total) ? 0 : total;
    } catch (err) {
      console.warn(`  ⚠ ${cat} query failed: ${err.message}`);
      results[cat] = 0;
    }
    await sleep(REQUEST_DELAY_MS);
  }

  return results;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log("📍 Loading district GeoJSON files…");
  const districts = [];
  for (const path of GEOJSON_FILES) {
    const gj = JSON.parse(readFileSync(path, "utf8"));
    for (const feat of gj.features) {
      const { district, city } = feat.properties;
      const centroid = centroidOf(feat.geometry);
      if (!centroid) {
        console.warn(`  ⚠ No centroid for ${city} ${district} — skipping`);
        continue;
      }
      districts.push({ city: nfkc(city), district: nfkc(district), centroid });
    }
  }
  console.log(`  → Loaded ${districts.length} districts`);

  if (DRY_RUN) {
    console.log("\n🔍 Dry-run mode — showing districts and centroids only:");
    for (const d of districts) {
      console.log(`  ${d.city} ${d.district}: lat=${d.centroid.lat.toFixed(4)} lng=${d.centroid.lng.toFixed(4)}`);
    }
    console.log("\nDone (dry-run). No Overpass queries were made.");
    return;
  }

  console.log(`\n🌐 Fetching POI counts from Overpass API for ${districts.length} districts…`);
  console.log("  (This may take several minutes — ~7 API calls × delay per district)\n");

  const output = {};
  let processed = 0;

  for (const { city, district, centroid } of districts) {
    const key = `${city}::${district}`;
    console.log(`  [${++processed}/${districts.length}] ${key}…`);
    try {
      const counts = await fetchPoiCounts(centroid);
      output[key] = counts;
      console.log(`    → restaurant:${counts.restaurant} sport:${counts.sport} cinema:${counts.cinema} supermarket:${counts.supermarket} convenience:${counts.convenience} park:${counts.park} hospital:${counts.hospital}`);
    } catch (err) {
      console.warn(`  ❌ Failed for ${key}: ${err.message}`);
      output[key] = { restaurant: 0, sport: 0, cinema: 0, supermarket: 0, convenience: 0, park: 0, hospital: 0 };
    }
  }

  console.log(`\n✍ Writing ${OUTPUT_PATH}…`);
  writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2), "utf8");
  console.log(`✅ Done — ${Object.keys(output).length} districts written to poi-scores.json`);
}

main().catch(err => {
  console.error("Fatal:", err);
  process.exit(1);
});
