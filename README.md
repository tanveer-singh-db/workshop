# AQI Analysis Pipeline

A SQL pipeline that transforms raw Air Quality Index (AQI) data from Indian monitoring stations into enriched, analysis-ready tables with pollution source attribution.

## Data

- **`sample_data/raw_aqi_data.csv`** — Raw AQI readings from monitoring stations across India (pollutant min/max/avg values per station).
- **`sample_data/genie_context`** — Reference context describing pollutant sources, used to derive the attribution matrix.

## Pipeline

Run the SQL files in `code/` sequentially. Each step creates a table consumed by the next.

| Step | File | Input | Output | Description |
|------|------|-------|--------|-------------|
| 1 | `01_aqi_cleaned.sql` | raw CSV | `aqi_cleaned` | Load and clean raw data — cast types, handle NAs, trim whitespace |
| 2 | `02_aqi_pivoted.sql` | `aqi_cleaned` | `aqi_pivoted` | Pivot from one-row-per-pollutant to one-row-per-station with columnar pollutant values |
| 3 | `03_aqi_aggregated.sql` | `aqi_pivoted` | `aqi_final` | Compute per-pollutant severity using India AQI breakpoints, derive overall AQI status |
| 4 | `04_aqi_source_attribution.sql` | `aqi_final` | `aqi_enriched` | Attribute pollution to source categories (vehicular, industrial, agricultural, construction, residential, natural) using severity-weighted scores |

## Analysis Queries

**`code/aqi_analysis.sql`** contains standalone SELECT queries for ad-hoc analysis on `aqi_enriched`:

1. **Emission contributors by state** — Aggregates raw severity scores per state, normalized to percentages
2. **Emission contributors by city** — Same breakdown at city level
3. **Top polluted cities by AQI status** — Station counts per AQI category by city
4. **Worst states ranked** — States ordered by average pollution severity

### Why raw scores matter

The `_pct` columns in `aqi_enriched` are normalized per station. Averaging percentages across stations would give equal weight to lightly and heavily polluted stations. The `_raw` columns (e.g. `vehicular_raw`) preserve severity-weighted scores, allowing correct aggregation: `SUM(raw) / SUM(total_raw)` at any geographic level.