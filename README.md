# 實價登錄雷達 (Real Price Radar)

Taiwan real estate analytics powered by government-mandated transaction data (實價登錄).

## What This Does

- **Actual transaction prices** — not listing prices, what people actually paid
- **Map view** — choropleth by price/坪, transaction dot layer (MapLibre GL JS)
- **Price history** — track same-building prices over time
- **Walkability scores** — MRT stations, schools, parks via OpenStreetMap

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Node.js + TypeScript + Express |
| Database | PostgreSQL + PostGIS |
| Frontend | Astro + MapLibre GL JS |
| Data Source | 內政部 plvr.land.moi.gov.tw open data |

## Quick Start

```bash
# Start PostGIS
docker compose up -d db

# Backend
cd backend && npm install && npm run dev

# Frontend
cd frontend && npm install && npm run dev
```

## Data Pipeline

Transaction data is published by 內政部 on the 1st, 11th, and 21st of each month.

```bash
# Download latest batch
cd backend && npm run pipeline:download

# Import to PostGIS
cd backend && npm run pipeline:import
```

## Project Structure

```
├── backend/          # Express API + data pipeline
│   └── src/
│       ├── api/      # REST endpoints
│       ├── db/       # PostGIS schema + migrations
│       └── pipeline/ # 實價登錄 CSV import
├── frontend/         # Astro + MapLibre map
│   └── src/
│       ├── pages/    # Astro pages
│       └── components/ # Map, search, filters
├── scripts/          # Utility scripts
└── docker-compose.yml
```

## Data Sources

| Source | Data | Frequency |
|--------|------|-----------|
| 內政部 plvr.land.moi.gov.tw | Transaction records | Monthly (1st/11th/21st) |
| OpenStreetMap Overpass | MRT, schools, parks | On-demand |

## License

MIT
