# 實價登錄雷達 (Real Price Radar)

Real Price Radar answers one focused question:

> Given a buyer's budget, commute anchor, and priorities, which districts offer the best cost-quality tradeoff, and why?

**Live demo:** https://copilot-autogent.github.io/realestate-radar/

## Product

This is a completed **n=1 district matcher** for the current target area:

- **Coverage:** 台北市, 新北市, and 桃園市. The interface and district boundary data are intentionally limited to these three cities.
- **Wizard-first flow:** the 首購族地區配對精靈 collects a total-budget tier, down-payment readiness, commute hub and time limit, then returns up to three ranked district matches.
- **Ranked scorecard:** after matching, districts are ranked with user-adjustable weights and an explanation of each result.
- **Five scoring dimensions:** affordability, commute convenience, purchasable space, building newness, and facilities.
- **Map drill-down:** selecting a ranked district opens its transaction map, filters, boundary heatmap, and supporting district-level details.

The product is in **maintenance-only posture**. The repository does not carry a feature roadmap or new feature commitments.

## Tech stack

| Layer | Technology |
|-------|------------|
| Backend | Node.js + TypeScript + Express |
| Database | PostgreSQL + PostGIS |
| Frontend | Astro + MapLibre GL JS |
| Data source | 內政部 `plvr.land.moi.gov.tw` open data |

## GitHub Pages demo and data scope

The live demo is a static Astro site and does not require the backend at runtime. It loads the latest committed `frontend/public/data/transactions.json`, together with the district boundaries and supporting JSON data under `frontend/public/data/`.

The weekly [data pipeline workflow](.github/workflows/pipeline.yml) downloads the latest 內政部 sales batch, imports it into PostGIS, geocodes pending records, exports geocoded transactions to `frontend/public/data/transactions.json`, and commits changed data. The [Pages deployment workflow](.github/workflows/deploy.yml) builds and publishes the frontend when the relevant source or data changes.

內政部 publishes transaction batches on the 1st, 11th, and 21st of each month. The pipeline runs weekly; the site displays the latest transaction date found in the committed JSON data.

## Local development

Requirements: Node.js 20+ and Docker for the PostGIS-backed backend.

```bash
# From the repository root
npm ci

# Run the frontend and backend together
npm run dev
```

The static frontend can be developed without PostGIS because it reads the committed JSON data. To run the backend API and live data pipeline locally:

```bash
# From the repository root
docker compose up -d db
npm run dev -w backend

# In another terminal
npm run dev -w frontend
```

The database defaults match `docker-compose.yml`: host `localhost`, port `5432`, database `realestate_radar`, user `radar`, and password `radar_dev`. Override them with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` when using another PostGIS instance.

## Data pipeline commands

Run these from the repository root. The import and export steps require `DATA_SOURCE=live` as a safety guard.

```bash
# Download the current bulk ZIP and extract sales CSVs to data/downloads/
npm run pipeline:download -w backend

# Import downloaded CSVs, geocode pending records, and export transactions.json
DATA_SOURCE=live npm run pipeline:live -w backend
```

For individual live steps:

```bash
DATA_SOURCE=live npm run pipeline:import -w backend
npm run pipeline:geocode -w backend
DATA_SOURCE=live npm run pipeline:export -w backend
```

`pipeline:live` performs download, incremental PostGIS import, geocoding, and static JSON export in that order. The generated download and geocode-cache files are ignored by Git. Do not run the import or export steps without `DATA_SOURCE=live`; for frontend-only work, use the committed `frontend/public/data/transactions.json` instead.

## Project structure

```text
├── backend/          # Express API, PostGIS schema, and data pipeline
│   └── src/
│       ├── api/      # REST endpoints
│       ├── db/       # schema and migrations
│       └── pipeline/ # 內政部 CSV import, geocoding, and export
├── frontend/         # Astro + MapLibre application
│   ├── src/pages/    # wizard, scorecard, and page orchestration
│   ├── src/components/ # map and district detail views
│   └── public/data/  # committed static data and district boundaries
├── scripts/          # data and metadata utilities
└── docker-compose.yml # local PostGIS service
```

## License

MIT
