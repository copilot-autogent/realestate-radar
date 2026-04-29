/**
 * Batch geocode transactions that have an address but no coordinates.
 *
 * Reads transactions with address IS NOT NULL AND lat IS NULL,
 * geocodes each via Nominatim (rate-limited), and writes
 * lat, lon, and PostGIS geom back to the database.
 *
 * Usage:
 *   npm run pipeline:geocode -w backend
 *   npm run pipeline:geocode -w backend -- --limit 100
 *   npm run pipeline:geocode -w backend -- --dry-run
 */

import { query, close } from "../db/connection.js";
import { geocodeAddress, flushCache, cacheSize } from "./geocode.js";

interface PendingRow {
  id: number;
  address: string;
  city: string;
  district: string;
}

function parseArgs(): { limit: number; dryRun: boolean } {
  const args = process.argv.slice(2);
  let limit = 500;
  let dryRun = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--limit" && args[i + 1]) {
      limit = parseInt(args[++i], 10);
    } else if (args[i].startsWith("--limit=")) {
      limit = parseInt(args[i].split("=")[1], 10);
    } else if (args[i] === "--dry-run") {
      dryRun = true;
    }
  }

  return { limit, dryRun };
}

async function main(): Promise<void> {
  const { limit, dryRun } = parseArgs();

  console.log(`[geocode-pending] Starting (limit=${limit}${dryRun ? ", dry-run" : ""})`);
  console.log(`[geocode-pending] Cache has ${cacheSize()} entries`);

  const { rows } = await query<PendingRow>(
    `SELECT id, address, city, district
     FROM transactions
     WHERE lat IS NULL
       AND address IS NOT NULL
       AND address != ''
     ORDER BY id
     LIMIT $1`,
    [limit]
  );

  if (rows.length === 0) {
    console.log("[geocode-pending] No pending records — all coordinates are filled!");
    await close();
    return;
  }

  console.log(`[geocode-pending] Found ${rows.length} records to geocode`);

  let success = 0;
  let failed = 0;
  let cached = 0;

  for (let i = 0; i < rows.length; i++) {
    const { id, address, city, district } = rows[i];

    // Prepend city+district if the address doesn't already contain them
    // (PLVR addresses often start with just the road name)
    const enriched = address.startsWith(city) || address.startsWith(district)
      ? address
      : `${city}${district}${address}`;

    const result = await geocodeAddress(enriched);

    if (result) {
      if (!dryRun) {
        await query(
          `UPDATE transactions
           SET lat  = $1,
               lon  = $2,
               geom = ST_SetSRID(ST_MakePoint($2, $1), 4326)
           WHERE id = $3`,
          [result.lat, result.lon, id]
        );
      }
      success++;
    } else {
      failed++;
    }

    // Progress log every 20 records
    if ((i + 1) % 20 === 0 || i === rows.length - 1) {
      const pct = (((i + 1) / rows.length) * 100).toFixed(0);
      console.log(`[geocode-pending] ${i + 1}/${rows.length} (${pct}%) — ok=${success} miss=${failed}`);
    }
  }

  flushCache();

  console.log(`
[geocode-pending] Complete`);
  console.log(`  Geocoded:  ${success}`);
  console.log(`  Failed:    ${failed}`);
  console.log(`  Cache now: ${cacheSize()} entries`);
  if (dryRun) console.log("  (dry run — no DB changes made)");

  await close();
}

await main();
