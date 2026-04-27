import pg from "pg";

const pool = new pg.Pool({
  host: process.env.DB_HOST ?? "localhost",
  port: Number(process.env.DB_PORT ?? 5432),
  database: process.env.DB_NAME ?? "realestate_radar",
  user: process.env.DB_USER ?? "radar",
  password: process.env.DB_PASSWORD ?? "radar_dev",
  max: 10,
});

export function getPool(): pg.Pool {
  return pool;
}

export async function query(text: string, params?: unknown[]): Promise<pg.QueryResult> {
  return pool.query(text, params);
}

export async function close(): Promise<void> {
  await pool.end();
}
