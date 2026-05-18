# 實價登錄雷達 (Real Price Radar)

Taiwan real estate analytics powered by government-mandated transaction data (實價登錄).

**🔗 Live Demo**: https://copilot-autogent.github.io/realestate-radar/

## What This Does

- **Actual transaction prices** — not listing prices, what people actually paid
- **Interactive map** — choropleth heatmap by price/坪, transaction dot layer (MapLibre GL JS)
- **City & district filters** — all 6 major cities: 台北市, 新北市, 桃園市, 台中市, 台南市, 高雄市
- **Unit price filter** — filter by 萬/坪 range
- **Building type filter** — 住宅大樓, 華廈, 公寓, 透天厝, 套房
- **Date range filter** — presets (近半年 / 近1年 / 近2年) + custom date inputs
- **Address search** — keyword search across transaction addresses
- **Stats panel** — match count, median and average price/坪 for current filter
- **Price distribution histogram** — 10-bin unit price distribution chart
- **District trend chart** — click any district to see quarterly median price trend

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Node.js + TypeScript + Express |
| Database | PostgreSQL + PostGIS |
| Frontend | Astro + MapLibre GL JS |
| Data Source | 內政部 plvr.land.moi.gov.tw open data |

## Demo Mode (GitHub Pages)

The live demo runs fully in the browser — no backend required. It uses a static sample dataset of ~965 synthetic transactions across all 6 major cities with realistic price distributions. All filters work client-side.

## Full Backend Mode (Local Dev)

To run with real 內政部 data:

```bash
# Start PostGIS
docker compose up -d db

# Backend
cd backend && npm install && npm run dev

# Frontend
cd frontend && npm install && npm run dev
```

### Data Pipeline

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

## Roadmap

- [ ] Multi-city district choropleth (non-Taipei cities currently show grey)
- [ ] Total price filter (總價篩選) — filter by buyer budget in 萬
- [ ] Transaction list panel — sortable table of results below histogram
- [ ] Real 內政部 data pipeline integration

## License

MIT
