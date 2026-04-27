import { Router, type Request, type Response } from "express";
import { query } from "../db/connection.js";

export function transactionsRouter(): Router {
  const router = Router();

  /**
   * GET /api/transactions
   * Query transactions with filters.
   *
   * Query params:
   *   city - filter by city (e.g. 台北市)
   *   district - filter by district (e.g. 大安區)
   *   minPrice - minimum unit price (元/坪)
   *   maxPrice - maximum unit price (元/坪)
   *   dateFrom - start date (YYYY-MM-DD)
   *   dateTo - end date (YYYY-MM-DD)
   *   buildingType - filter by building type
   *   limit - max results (default 100, max 1000)
   *   offset - pagination offset
   */
  router.get("/", async (req: Request, res: Response) => {
    const {
      city, district, minPrice, maxPrice,
      dateFrom, dateTo, buildingType,
      limit: limitStr, offset: offsetStr,
    } = req.query;

    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (city) {
      conditions.push(`city = $${paramIdx++}`);
      params.push(city);
    }
    if (district) {
      conditions.push(`district = $${paramIdx++}`);
      params.push(district);
    }
    if (minPrice) {
      conditions.push(`unit_price >= $${paramIdx++}`);
      params.push(Number(minPrice));
    }
    if (maxPrice) {
      conditions.push(`unit_price <= $${paramIdx++}`);
      params.push(Number(maxPrice));
    }
    if (dateFrom) {
      conditions.push(`transaction_date >= $${paramIdx++}`);
      params.push(dateFrom);
    }
    if (dateTo) {
      conditions.push(`transaction_date <= $${paramIdx++}`);
      params.push(dateTo);
    }
    if (buildingType) {
      conditions.push(`building_type = $${paramIdx++}`);
      params.push(buildingType);
    }

    const limit = Math.min(Number(limitStr) || 100, 1000);
    const offset = Number(offsetStr) || 0;

    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    try {
      const result = await query(
        `SELECT id, city, district, address, transaction_date, transaction_type,
                total_price, unit_price, area_sqm, area_ping,
                building_type, floors_total, floor, build_year,
                rooms, halls, bathrooms,
                lat, lon
         FROM transactions
         ${where}
         ORDER BY transaction_date DESC
         LIMIT $${paramIdx++} OFFSET $${paramIdx++}`,
        [...params, limit, offset]
      );

      res.json({
        count: result.rowCount,
        data: result.rows,
      });
    } catch (err) {
      console.error("Query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  /**
   * GET /api/transactions/stats
   * District-level price statistics (for choropleth map)
   */
  router.get("/stats", async (_req: Request, res: Response) => {
    try {
      const result = await query(
        `SELECT city, district, tx_count, median_unit_price, avg_unit_price,
                min_unit_price, max_unit_price, earliest_date, latest_date
         FROM district_price_stats
         ORDER BY median_unit_price DESC`
      );
      res.json({ data: result.rows });
    } catch (err) {
      console.error("Stats query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  /**
   * GET /api/transactions/history
   * Price history for a specific address/building
   */
  router.get("/history", async (req: Request, res: Response) => {
    const { address, city, district } = req.query;
    if (!address) {
      res.status(400).json({ error: "address parameter required" });
      return;
    }

    const conditions = [`address LIKE $1`];
    const params: unknown[] = [`%${address}%`];
    let paramIdx = 2;

    if (city) {
      conditions.push(`city = $${paramIdx++}`);
      params.push(city);
    }
    if (district) {
      conditions.push(`district = $${paramIdx++}`);
      params.push(district);
    }

    try {
      const result = await query(
        `SELECT id, city, district, address, transaction_date,
                total_price, unit_price, area_ping, floor, building_type
         FROM transactions
         WHERE ${conditions.join(" AND ")}
         ORDER BY transaction_date ASC`,
        params
      );
      res.json({
        address,
        count: result.rowCount,
        history: result.rows,
      });
    } catch (err) {
      console.error("History query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  /**
   * GET /api/transactions/geojson
   * GeoJSON FeatureCollection for map rendering
   */
  router.get("/geojson", async (req: Request, res: Response) => {
    const { city, district, minPrice, maxPrice, limit: limitStr } = req.query;

    const conditions: string[] = ["lat IS NOT NULL", "lon IS NOT NULL"];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (city) {
      conditions.push(`city = $${paramIdx++}`);
      params.push(city);
    }
    if (district) {
      conditions.push(`district = $${paramIdx++}`);
      params.push(district);
    }
    if (minPrice) {
      conditions.push(`unit_price >= $${paramIdx++}`);
      params.push(Number(minPrice));
    }
    if (maxPrice) {
      conditions.push(`unit_price <= $${paramIdx++}`);
      params.push(Number(maxPrice));
    }

    const limit = Math.min(Number(limitStr) || 5000, 10000);

    try {
      const result = await query(
        `SELECT id, lon, lat, unit_price, total_price, area_ping,
                building_type, transaction_date, address, city, district
         FROM transactions
         WHERE ${conditions.join(" AND ")}
         ORDER BY transaction_date DESC
         LIMIT $${paramIdx}`,
        [...params, limit]
      );

      const geojson = {
        type: "FeatureCollection" as const,
        features: result.rows.map(row => ({
          type: "Feature" as const,
          geometry: {
            type: "Point" as const,
            coordinates: [row.lon, row.lat],
          },
          properties: {
            id: row.id,
            unitPrice: row.unit_price,
            totalPrice: row.total_price,
            areaPing: row.area_ping,
            buildingType: row.building_type,
            date: row.transaction_date,
            address: row.address,
            city: row.city,
            district: row.district,
          },
        })),
      };

      res.json(geojson);
    } catch (err) {
      console.error("GeoJSON query error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  return router;
}
