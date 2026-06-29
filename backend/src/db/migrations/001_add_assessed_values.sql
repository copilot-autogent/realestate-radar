-- Migration: Add assessed land value (公告現值) columns and district table
-- Issue #76: Data join pipeline for 公告現值 from 內政部地政司 公告地價 dataset

-- Add assessed value columns to existing transactions
ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS assessed_value_per_sqm NUMERIC(12,2),
  ADD COLUMN IF NOT EXISTS assessed_to_market_ratio NUMERIC(8,4);

-- District-level 公告現值 aggregated from 內政部地政司 公告地價 dataset
CREATE TABLE IF NOT EXISTS district_assessed_values (
  id SERIAL PRIMARY KEY,
  city TEXT NOT NULL,
  district TEXT NOT NULL,
  year INTEGER NOT NULL,
  median_assessed_value_per_sqm NUMERIC(12,2) NOT NULL,
  parcel_count INTEGER NOT NULL DEFAULT 0,
  source_file TEXT,
  imported_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(city, district, year)
);

CREATE INDEX IF NOT EXISTS idx_district_assessed_city_district
  ON district_assessed_values(city, district);
