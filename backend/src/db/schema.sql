-- 實價登錄雷達 PostGIS Schema
-- Designed for 內政部 實價登錄 transaction data

CREATE EXTENSION IF NOT EXISTS postgis;

-- Lookup: Taiwan districts (鄉鎮市區)
CREATE TABLE IF NOT EXISTS districts (
  id SERIAL PRIMARY KEY,
  city TEXT NOT NULL,           -- 縣市 e.g. 台北市
  district TEXT NOT NULL,       -- 鄉鎮市區 e.g. 大安區
  geom GEOMETRY(MultiPolygon, 4326),
  UNIQUE(city, district)
);

-- Core: real estate transactions (實價登錄)
CREATE TABLE IF NOT EXISTS transactions (
  id SERIAL PRIMARY KEY,

  -- Location
  city TEXT NOT NULL,                  -- 縣市
  district TEXT NOT NULL,              -- 鄉鎮市區
  address TEXT,                        -- 土地位置建物門牌
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  geom GEOMETRY(Point, 4326),

  -- Transaction details
  transaction_date DATE NOT NULL,      -- 交易年月日
  transaction_type TEXT NOT NULL,      -- 交易標的 (房地, 土地, 建物, 車位)
  total_price BIGINT NOT NULL,         -- 總價元
  unit_price INTEGER,                  -- 單價元/坪
  area_sqm NUMERIC(10,2),             -- 建物移轉總面積(平方公尺)
  area_ping NUMERIC(10,2) GENERATED ALWAYS AS (area_sqm * 0.3025) STORED,

  -- Building info
  building_type TEXT,                  -- 建物型態 (住宅大樓, 公寓, 透天厝, etc.)
  floors_total INTEGER,               -- 總樓層數
  floor TEXT,                          -- 移轉層次
  build_year INTEGER,                  -- 建築完成年月 (ROC year)
  rooms INTEGER,                       -- 建物現況格局-房
  halls INTEGER,                       -- 建物現況格局-廳
  bathrooms INTEGER,                   -- 建物現況格局-衛

  -- Parking
  has_parking BOOLEAN DEFAULT FALSE,
  parking_type TEXT,                   -- 車位類別
  parking_price BIGINT,                -- 車位總價元
  parking_area NUMERIC(10,2),          -- 車位移轉總面積(平方公尺)

  -- Metadata
  land_use TEXT,                       -- 都市土地使用分區 (住, 商, 工)
  note TEXT,                           -- 備註
  serial_number TEXT,                  -- 編號 (for dedup)
  source_file TEXT,                    -- Which CSV batch this came from
  imported_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(serial_number)
);

-- Spatial index on transaction locations
CREATE INDEX IF NOT EXISTS idx_transactions_geom ON transactions USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_transactions_city_district ON transactions(city, district);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_address ON transactions(address);
CREATE INDEX IF NOT EXISTS idx_transactions_unit_price ON transactions(unit_price);

-- POI data (MRT stations, schools, parks)
CREATE TABLE IF NOT EXISTS pois (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,              -- mrt_station, school, park, hospital, market
  subcategory TEXT,                    -- e.g. elementary, junior_high
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  geom GEOMETRY(Point, 4326),
  metadata JSONB DEFAULT '{}'::jsonb,  -- Extra info (MRT line, school type, etc.)
  source TEXT,                         -- osm, motc, manual
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pois_geom ON pois USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_pois_category ON pois(category);

-- Import tracking
CREATE TABLE IF NOT EXISTS import_log (
  id SERIAL PRIMARY KEY,
  source TEXT NOT NULL,                -- 'plvr', 'osm', etc.
  filename TEXT,
  record_count INTEGER,
  started_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  status TEXT DEFAULT 'running',       -- running, success, failed
  error TEXT
);

-- Materialized view: price stats per district (for choropleth)
CREATE MATERIALIZED VIEW IF NOT EXISTS district_price_stats AS
SELECT
  city,
  district,
  COUNT(*) AS tx_count,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_price) AS median_unit_price,
  AVG(unit_price)::INTEGER AS avg_unit_price,
  MIN(unit_price) AS min_unit_price,
  MAX(unit_price) AS max_unit_price,
  MIN(transaction_date) AS earliest_date,
  MAX(transaction_date) AS latest_date
FROM transactions
WHERE unit_price IS NOT NULL AND unit_price > 0
GROUP BY city, district;

CREATE UNIQUE INDEX IF NOT EXISTS idx_district_stats_city_district
  ON district_price_stats(city, district);
