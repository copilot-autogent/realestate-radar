# Geospatial Pipeline Methodology — MSR Open-Data Topology Patterns

**Purpose**: Document techniques for inferring topological relationships from open geospatial data (內政部 district boundaries, transaction coordinates) without requiring curated adjacency metadata.

**Context**: Real Price Radar needs district-level analysis (median prices, affordability overlays) and future adjacency-aware queries ("show transactions in districts neighboring Xinyi"). The Taiwan government provides GeoJSON boundaries but no explicit adjacency graph.

---

## Problem Space

### What We Have
- 內政部 實價登錄 transaction data: CSV with lat/lon coordinates, district names (text labels)
- District boundary polygons: Available via Taiwan Open Data portal as GeoJSON/Shapefile
- Map rendering: MapLibre GL JS displays choropleth layers client-side

### What We Need
1. **Point-in-polygon matching**: Given a transaction's lat/lon, determine which district polygon it belongs to (for aggregation)
2. **Adjacency inference**: Given district boundaries, compute which districts share borders (for "find similar neighborhoods nearby" feature)
3. **Boundary simplification**: Reduce polygon complexity for client-side rendering performance without losing topology

---

## MSR-Inspired Techniques for Geospatial Topology

### 1. Point-in-Polygon: Ray-Casting Algorithm

**Pattern**: MSR uses commit-file mappings inferred from git log analysis. Similarly, we infer transaction-district mappings from geometry alone.

**Implementation**:
- **Client-side** (JavaScript): Use [Turf.js](https://turfjs.org/) \`booleanPointInPolygon()\` for filtering already-loaded GeoJSON
- **Server-side** (PostGIS): \`ST_Contains(district_geom, ST_Point(lon, lat))\` for bulk processing during data import

**Trade-off**:
- Client-side: No backend required, but processes only visible transactions (filter-then-match)
- Server-side: Pre-computes district_id for all transactions, enables SQL queries like \`WHERE district_id IN (...)\`

**Recommendation**: Hybrid — pre-compute district_id during CSV import (PostGIS backend task), store in transaction JSON, eliminate client-side point-in-polygon overhead.

---

### 2. Adjacency Graph: Boundary Intersection Detection

**Pattern**: MSR infers developer collaboration networks from commit co-authorship. Similarly, infer district adjacency from geometric overlap.

**Algorithm** (PostGIS):
\`\`\`sql
-- Find districts that share a boundary (not just touch at a point)
SELECT 
  d1.district_name AS district_a,
  d2.district_name AS district_b
FROM districts d1
JOIN districts d2 ON d1.id < d2.id  -- Avoid duplicates (A-B vs B-A)
WHERE ST_Touches(d1.geom, d2.geom)  -- Share boundary
  AND ST_Dimension(ST_Intersection(d1.geom, d2.geom)) = 1;  -- Line, not point
\`\`\`

**Why \`ST_Dimension = 1\`?**  
- \`ST_Touches\` includes districts that meet at a single corner point (e.g., Taipei/New Taipei/Keelung tripoint)
- Filtering for \`ST_Dimension(intersection) = 1\` ensures shared *edge*, not just vertex

**Output**: Adjacency list \`{district_id: [neighbor_ids]}\` stored as JSON, loaded by frontend for "expand search to neighboring districts" feature.

---

### 3. Topology Validation: Detect Invalid Boundaries

**Pattern**: MSR uses commit graph validation (orphaned commits, merge conflicts). Similarly, validate geospatial topology before import.

**PostGIS Checks**:
\`\`\`sql
-- Check for self-intersecting polygons
SELECT district_name FROM districts WHERE NOT ST_IsValid(geom);

-- Check for gaps between districts (should cover entire city)
SELECT ST_Area(
  ST_Difference(
    ST_ConvexHull(ST_Collect(geom)),
    ST_Union(geom)
  )
) AS uncovered_area
FROM districts;

-- Check for overlapping districts (should be disjoint)
SELECT d1.district_name, d2.district_name
FROM districts d1, districts d2
WHERE d1.id < d2.id
  AND ST_Overlaps(d1.geom, d2.geom);
\`\`\`

**Why this matters**: Taiwan government GeoJSON occasionally has encoding errors (simplified boundaries that create gaps). Early validation prevents choropleth rendering bugs.

---

### 4. Boundary Simplification: Douglas-Peucker Algorithm

**Pattern**: MSR uses text diff algorithms (Myers, Patience) to detect semantic changes while ignoring whitespace. Similarly, simplify polygon boundaries to preserve shape while reducing vertex count.

**PostGIS**:
\`\`\`sql
-- Simplify district boundaries to 10m tolerance for client-side rendering
SELECT 
  district_id,
  district_name,
  ST_SimplifyPreserveTopology(geom, 10) AS simplified_geom
FROM districts;
\`\`\`

**Trade-off**:
- Too aggressive (tolerance >100m): Districts lose recognizable shape
- Too conservative (tolerance <5m): Minimal size reduction, client-side map still slow

**Recommendation**: 10-20m tolerance for Taipei districts (balances file size vs. visual accuracy at zoom level 11-14).

---

## Data Pipeline Architecture

\`\`\`
[內政部 CSV] → [Backend ETL] → [PostGIS Database]
                    ↓
            1. Parse lat/lon
            2. ST_Contains → district_id
            3. Store transaction + district_id
                    ↓
            [GeoJSON Export] → [Static JSON in frontend/public/]
                    ↓
            [MapLibre GL JS] → Render choropleth + transactions
\`\`\`

### Key Decisions
1. **Pre-compute district_id**: Avoids client-side point-in-polygon for every transaction
2. **Export simplified GeoJSON**: Reduce client-side bundle size (from ~2MB to ~200KB for Taipei)
3. **Adjacency JSON**: Separate file \`district-adjacency.json\` loaded on-demand for "expand search" feature

---

## Implementation Phases

### Phase A: Current State (Client-Side Only)
- ✅ MapLibre displays transaction dots + choropleth
- ✅ Frontend computes district medians from transaction array
- ❌ No point-in-polygon matching (relies on transaction CSV having pre-labeled \`district\` field from government data)
- ❌ No adjacency awareness

### Phase B: Backend Setup (Future)
1. Set up PostgreSQL + PostGIS
2. Import district GeoJSON → \`districts\` table
3. Import transaction CSV → \`transactions\` table with \`ST_Contains\` join
4. Validate topology (ST_IsValid, check overlaps/gaps)
5. Generate adjacency JSON via ST_Touches query

### Phase C: Optimizations (Future)
1. Simplify district boundaries (ST_SimplifyPreserveTopology)
2. Export pre-aggregated district stats (median price, transaction count) as static JSON
3. Add spatial index: \`CREATE INDEX idx_transactions_geom ON transactions USING GIST (geom);\`
4. Implement "expand to neighbors" UI affordance

---

## Cross-Pollination: MSR Analog Table

| MSR Technique | Geospatial Analog | Use Case |
|---|---|---|
| Commit → File mapping (git log) | Transaction → District (ST_Contains) | Aggregate transactions by district |
| Co-authorship graph (commit metadata) | Adjacency graph (ST_Touches) | "Show similar neighborhoods nearby" |
| Diff algorithm (Myers, Patience) | Boundary simplification (Douglas-Peucker) | Reduce GeoJSON size |
| Merge conflict detection | Topology validation (ST_IsValid, overlap checks) | Catch bad government data |
| Repository clone graph | District hierarchy (city → district → neighborhood) | Drill-down UI (future) |

---

## References

1. **PostGIS Documentation**: [Spatial Relationships](https://postgis.net/docs/reference.html#Spatial_Relationships)
2. **Turf.js**: [booleanPointInPolygon](https://turfjs.org/docs/#booleanPointInPolygon)
3. **Douglas-Peucker Algorithm**: [Wikipedia](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm)
4. **Taiwan Open Data Portal**: [內政部不動產實價登錄](https://plvr.land.moi.gov.tw/)

---

## Next Steps

1. File Phase B as separate issue: "Backend ETL: PostgreSQL + PostGIS setup" (tier:2-standard)
2. File Phase C optimization as follow-up: "Geospatial optimizations: simplification + spatial index" (tier:1-trivial)
3. Consider: Do we need full backend now, or can client-side + static GeoJSON suffice until user demand justifies PostGIS setup?

**Decision**: Defer backend until transaction count >100K or users request adjacency-based search. Current static approach works for MVP.
