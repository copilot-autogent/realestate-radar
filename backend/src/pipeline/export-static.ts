/**
 * Export live PostGIS transaction data to GeoJSON for the static (GitHub Pages) site.
 *
 * Produces frontend/public/data/sample-transactions.json in the same format
 * as the synthetic sample so the Astro/MapLibre frontend works without changes.
 *
 * Only exports geocoded records (lat/lon NOT NULL).
 * Run after import-plvr.ts has processed the latest CSV batch.
 *
 * Usage:
 *   DATA_SOURCE=live tsx src/pipeline/export-static.ts
 *
 * Environment variables:
 *   DATA_SOURCE=live    — required; prevents accidental overwrite of sample data
 *   EXPORT_LIMIT        — max records to export (default: 10000)
 *   EXPORT_DAYS         — rolling window in days; export only records within this
 *                         many days of the latest transaction (default: 730 = ~2 yrs)
 */

import { writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { query, close } from "../db/connection.js";

const EXPORT_LIMIT = parseInt(process.env.EXPORT_LIMIT ?? "10000", 10);
const EXPORT_DAYS = parseInt(process.env.EXPORT_DAYS ?? "730", 10);

const OUT_PATH = path.resolve(
  import.meta.dirname,
  "../../../../frontend/public/data/sample-transactions.json"
);

interface ExportRow {
  id: number;
  lon: number;
  lat: number;
  unit_price: number;
  total_price: number;
  area_ping: string | null;
  building_type: string | null;
  transaction_date: string;
  address: string | null;
  city: string;
  district: string;
  floor: string | null;
  floors_total: number | null;
  rooms: number | null;
  build_year: number | null;
  assessed_value_per_sqm: string | null;
  assessed_to_market_ratio: string | null;
}

async function exportStatic(): Promise<void> {
  console.log(`[export] Querying up to ${EXPORT_LIMIT} geocoded records (last ${EXPORT_DAYS} days)...`);

  const result = await query<ExportRow>(
    `SELECT
       id, lon, lat, unit_price, total_price, area_ping,
       building_type, transaction_date, address, city, district,
       floor, floors_total, rooms, build_year,
       assessed_value_per_sqm, assessed_to_market_ratio
     FROM transactions
     WHERE lat IS NOT NULL
       AND lon IS NOT NULL
       AND unit_price > 0
       AND transaction_date >= (
           SELECT MAX(transaction_date) - $1::interval
           FROM transactions
           WHERE lat IS NOT NULL
       )
     ORDER BY transaction_date DESC
     LIMIT $2`,
    [`${EXPORT_DAYS} days`, EXPORT_LIMIT]
  );

  const features = result.rows.map(row => ({
    type: "Feature" as const,
    geometry: {
      type: "Point" as const,
      coordinates: [row.lon, row.lat],
    },
    properties: {
      id: row.id,
      unitPrice: row.unit_price,
      totalPrice: row.total_price,
      areaPing: row.area_ping != null ? +Number(row.area_ping).toFixed(1) : null,
      buildingType: row.building_type,
      date: typeof row.transaction_date === "string"
        ? row.transaction_date.split("T")[0]
        : (row.transaction_date as unknown as Date).toISOString().split("T")[0],
      address: row.address,
      city: row.city,
      district: row.district,
      floor: row.floor,
      floorsTotal: row.floors_total,
      rooms: row.rooms,
      buildYear: row.build_year,
      assessedValuePerSqm: row.assessed_value_per_sqm != null
        ? +Number(row.assessed_value_per_sqm).toFixed(2) : null,
      assessedToMarketRatio: row.assessed_to_market_ratio != null
        ? +Number(row.assessed_to_market_ratio).toFixed(4) : null,
    },
  }));

  const geojson = { type: "FeatureCollection" as const, features };

  mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  writeFileSync(OUT_PATH, JSON.stringify(geojson));

  console.log(`[export] ${features.length} records → ${OUT_PATH}`);
  await close();
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  if (process.env.DATA_SOURCE !== "live") {
    console.error(
      "[error] DATA_SOURCE must be 'live' to run export-static.\n" +
      "        Set DATA_SOURCE=live to prevent accidental overwrite of sample data."
    );
    process.exit(1);
  }
  await exportStatic();
}

export { exportStatic };
