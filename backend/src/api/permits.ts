/**
 * /api/permits/:city — Quarterly permit time series
 *
 * Returns building permit (建照), occupancy permit (使照) and starts (開工)
 * data for a given city, from 2021Q1 to 2028Q4 (historical + projected).
 *
 * Data source: 內政部不動產資訊平台 E3030
 * Ingestion: run `npm run pipeline:permits` from the backend directory.
 */

import { Router, type Request, type Response } from "express";
import { query } from "../db/connection.js";

/** Validate a YYYYQN quarter string. */
function isValidQuarter(q: unknown): q is string {
  return typeof q === "string" && /^\d{4}Q[1-4]$/.test(q);
}

/** Compare two quarter strings chronologically. Returns negative/0/positive. */
function cmpQuarter(a: string, b: string): number {
  const [ay, aq] = [parseInt(a.slice(0, 4)), parseInt(a.slice(5))];
  const [by, bq] = [parseInt(b.slice(0, 4)), parseInt(b.slice(5))];
  return ay !== by ? ay - by : aq - bq;
}

export function permitsRouter(): Router {
  const router = Router();

  /**
   * GET /api/permits/:city
   *
   * Path params:
   *   city — City name e.g. "台北市" (or "all" for 6 cities)
   *
   * Query params:
   *   from — start quarter, e.g. "2021Q1" (default: "2021Q1")
   *   to   — end quarter,   e.g. "2028Q4" (default: "2028Q4")
   *
   * Response: { quarters, first_projected_quarter, cities: { [city]: { building_permits, occupancy_permits, starts }[] } }
   */
  router.get("/:city", async (req: Request, res: Response) => {
    const city = req.params["city"] as string;

    // Validate and extract from/to query params (must be strings, not arrays)
    const rawFrom = Array.isArray(req.query["from"]) ? req.query["from"][0] : req.query["from"];
    const rawTo   = Array.isArray(req.query["to"])   ? req.query["to"][0]   : req.query["to"];
    const from = rawFrom ?? "2021Q1";
    const to   = rawTo   ?? "2028Q4";

    if (!isValidQuarter(from) || !isValidQuarter(to)) {
      res.status(400).json({ error: "Invalid quarter format. Use YYYYQN (e.g. 2024Q1)" });
      return;
    }
    if (cmpQuarter(from, to) > 0) {
      res.status(400).json({ error: "'from' must not be after 'to'" });
      return;
    }

    const cityFilter = city === "all" ? null : city;

    try {
      const result = await query<{
        city: string;
        quarter: string;
        quarter_year: number;
        quarter_num: number;
        building_permits: number;
        occupancy_permits: number;
        starts: number;
        is_projected: boolean;
      }>(
        `SELECT city, quarter, quarter_year, quarter_num,
                building_permits, occupancy_permits, starts, is_projected
         FROM permit_records
         WHERE district = ''
           AND (quarter_year > $1 OR (quarter_year = $1 AND quarter_num >= $2))
           AND (quarter_year < $3 OR (quarter_year = $3 AND quarter_num <= $4))
           ${cityFilter ? "AND city = $5" : ""}
         ORDER BY quarter_year, quarter_num, city`,
        cityFilter
          ? [
              parseInt(from.slice(0, 4), 10), parseInt(from.slice(5), 10),
              parseInt(to.slice(0, 4), 10), parseInt(to.slice(5), 10),
              cityFilter,
            ]
          : [
              parseInt(from.slice(0, 4), 10), parseInt(from.slice(5), 10),
              parseInt(to.slice(0, 4), 10), parseInt(to.slice(5), 10),
            ]
      );

      if ((result.rowCount ?? 0) === 0) {
        res.status(404).json({
          error: "No permit data found. Run the ingest pipeline first.",
          hint: "cd backend && npm run pipeline:permits",
        });
        return;
      }

      // Build ordered quarters list and per-city maps
      const quartersSet = new Set<string>();
      const byCity: Record<string, {
        building_permits: Map<string, number>;
        occupancy_permits: Map<string, number>;
        starts: Map<string, number>;
      }> = {};
      let firstProjectedQuarter: string | null = null;

      for (const row of result.rows) {
        quartersSet.add(row.quarter);
        if (!byCity[row.city]) {
          byCity[row.city] = {
            building_permits: new Map(),
            occupancy_permits: new Map(),
            starts: new Map(),
          };
        }
        const cityEntry = byCity[row.city]!;
        cityEntry.building_permits.set(row.quarter, Number(row.building_permits));
        cityEntry.occupancy_permits.set(row.quarter, Number(row.occupancy_permits));
        cityEntry.starts.set(row.quarter, Number(row.starts));

        if (row.is_projected && !firstProjectedQuarter) {
          firstProjectedQuarter = row.quarter;
        }
      }

      // Sort quarters chronologically
      const quarters = [...quartersSet].sort(cmpQuarter);

      // Assemble response
      const cities: Record<string, {
        building_permits: number[];
        occupancy_permits: number[];
        starts: number[];
      }> = {};

      for (const [c, maps] of Object.entries(byCity)) {
        cities[c] = {
          building_permits: quarters.map(q => maps.building_permits.get(q) ?? 0),
          occupancy_permits: quarters.map(q => maps.occupancy_permits.get(q) ?? 0),
          starts: quarters.map(q => maps.starts.get(q) ?? 0),
        };
      }

      res.json({
        quarters,
        first_projected_quarter: firstProjectedQuarter ?? quarters[quarters.length - 1],
        cities,
      });
    } catch (err) {
      console.error("Permits query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  /**
   * GET /api/permits/:city/district/:district
   *
   * District-level permit data.
   * District-level rows are only populated when the weighted E5010 allocation model ships.
   * Returns 404 with a hint when no district-level data is available.
   */
  router.get("/:city/district/:district", async (req: Request, res: Response) => {
    const city     = req.params["city"] as string;
    const district = req.params["district"] as string;

    const rawFrom = Array.isArray(req.query["from"]) ? req.query["from"][0] : req.query["from"];
    const rawTo   = Array.isArray(req.query["to"])   ? req.query["to"][0]   : req.query["to"];
    const from = rawFrom ?? "2021Q1";
    const to   = rawTo   ?? "2028Q4";

    if (!isValidQuarter(from) || !isValidQuarter(to)) {
      res.status(400).json({ error: "Invalid quarter format. Use YYYYQN (e.g. 2024Q1)" });
      return;
    }
    if (cmpQuarter(from, to) > 0) {
      res.status(400).json({ error: "'from' must not be after 'to'" });
      return;
    }

    try {
      const result = await query<{
        quarter: string;
        building_permits: number;
        occupancy_permits: number;
        starts: number;
        is_projected: boolean;
      }>(
        `SELECT quarter, building_permits, occupancy_permits, starts, is_projected
         FROM permit_records
         WHERE city = $1
           AND district = $2
           AND (quarter_year > $3 OR (quarter_year = $3 AND quarter_num >= $4))
           AND (quarter_year < $5 OR (quarter_year = $5 AND quarter_num <= $6))
         ORDER BY quarter_year, quarter_num`,
        [
          city, district,
          parseInt(from.slice(0, 4), 10), parseInt(from.slice(5), 10),
          parseInt(to.slice(0, 4), 10), parseInt(to.slice(5), 10),
        ]
      );

      if ((result.rowCount ?? 0) === 0) {
        res.status(404).json({
          error: `No district-level permit data for ${city} / ${district}.`,
          hint: "District-level data requires the weighted E5010 allocation model (future work).",
        });
        return;
      }

      const rows = result.rows;
      const quarters = rows.map(r => r.quarter);
      const firstProjectedQuarter = rows.find(r => r.is_projected)?.quarter
        ?? quarters[quarters.length - 1]!;

      res.json({
        city,
        district,
        quarters,
        first_projected_quarter: firstProjectedQuarter,
        building_permits: rows.map(r => Number(r.building_permits)),
        occupancy_permits: rows.map(r => Number(r.occupancy_permits)),
        starts: rows.map(r => Number(r.starts)),
      });
    } catch (err) {
      console.error("District permits query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  return router;
}
