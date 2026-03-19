-- 04_source_attribution.sql
-- Enrich AQI data with pollution source attribution percentages.
--
-- Each pollutant is mapped to source categories (vehicular, industrial,
-- agricultural, construction, residential, natural) based on the genie_context
-- "Main Sources" field. Per-pollutant weights are severity-scaled, then
-- normalized across all present pollutants to produce a percentage breakdown.
--
-- Attribution matrix (derived from genie_context):
--   PM2.5 → 30% vehicular, 25% residential (biomass), 25% industrial, 20% construction
--   PM10  → 25% vehicular (road dust), 25% construction, 25% industrial, 25% natural (wind)
--   NO2   → 50% vehicular, 25% industrial (power plants), 25% industrial (combustion)
--   SO2   → 100% industrial (coal, refineries, power plants)
--   CO    → 40% vehicular, 30% residential (stoves/generators), 30% industrial
--   O3    → 50% vehicular (NOx precursor), 50% industrial (VOC precursor)
--   NH3   → 100% agricultural

CREATE OR REPLACE TABLE aqi_enriched AS
WITH source_weights AS (
  SELECT
    *,

    -- Severity as the weight for each pollutant (0 if NULL / not measured)
    COALESCE(
      CASE
        WHEN pm2_5_avg IS NULL THEN 0
        WHEN pm2_5_avg <= 30  THEN 1 WHEN pm2_5_avg <= 60  THEN 2
        WHEN pm2_5_avg <= 90  THEN 3 WHEN pm2_5_avg <= 120 THEN 4
        WHEN pm2_5_avg <= 250 THEN 5 ELSE 6
      END, 0) AS w_pm25,

    COALESCE(
      CASE
        WHEN pm10_avg IS NULL THEN 0
        WHEN pm10_avg <= 50  THEN 1 WHEN pm10_avg <= 100 THEN 2
        WHEN pm10_avg <= 250 THEN 3 WHEN pm10_avg <= 350 THEN 4
        WHEN pm10_avg <= 430 THEN 5 ELSE 6
      END, 0) AS w_pm10,

    COALESCE(
      CASE
        WHEN no2_avg IS NULL THEN 0
        WHEN no2_avg <= 40  THEN 1 WHEN no2_avg <= 80  THEN 2
        WHEN no2_avg <= 180 THEN 3 WHEN no2_avg <= 280 THEN 4
        WHEN no2_avg <= 400 THEN 5 ELSE 6
      END, 0) AS w_no2,

    COALESCE(
      CASE
        WHEN so2_avg IS NULL THEN 0
        WHEN so2_avg <= 40   THEN 1 WHEN so2_avg <= 80   THEN 2
        WHEN so2_avg <= 380  THEN 3 WHEN so2_avg <= 800  THEN 4
        WHEN so2_avg <= 1600 THEN 5 ELSE 6
      END, 0) AS w_so2,

    COALESCE(
      CASE
        WHEN co_avg IS NULL THEN 0
        WHEN co_avg <= 1.0 THEN 1 WHEN co_avg <= 2.0 THEN 2
        WHEN co_avg <= 10  THEN 3 WHEN co_avg <= 17  THEN 4
        WHEN co_avg <= 34  THEN 5 ELSE 6
      END, 0) AS w_co,

    COALESCE(
      CASE
        WHEN ozone_avg IS NULL THEN 0
        WHEN ozone_avg <= 50  THEN 1 WHEN ozone_avg <= 100 THEN 2
        WHEN ozone_avg <= 168 THEN 3 WHEN ozone_avg <= 208 THEN 4
        WHEN ozone_avg <= 748 THEN 5 ELSE 6
      END, 0) AS w_ozone,

    COALESCE(
      CASE
        WHEN nh3_avg IS NULL THEN 0
        WHEN nh3_avg <= 200  THEN 1 WHEN nh3_avg <= 400  THEN 2
        WHEN nh3_avg <= 800  THEN 3 WHEN nh3_avg <= 1200 THEN 4
        WHEN nh3_avg <= 1800 THEN 5 ELSE 6
      END, 0) AS w_nh3

  FROM aqi_final
),

raw_scores AS (
  SELECT
    *,

    -- Total severity across all pollutants (used as denominator)
    (w_pm25 + w_pm10 + w_no2 + w_so2 + w_co + w_ozone + w_nh3) AS total_weight,

    -- Vehicular raw score
    -- PM2.5(30%) + PM10(25%) + NO2(50%) + CO(40%) + O3(50%)
    (w_pm25 * 0.30) + (w_pm10 * 0.25) + (w_no2 * 0.50) + (w_co * 0.40) + (w_ozone * 0.50) AS vehicular_raw,

    -- Industrial raw score
    -- PM2.5(25%) + PM10(25%) + NO2(50%) + SO2(100%) + CO(30%) + O3(50%)
    (w_pm25 * 0.25) + (w_pm10 * 0.25) + (w_no2 * 0.50) + (w_so2 * 1.00) + (w_co * 0.30) + (w_ozone * 0.50) AS industrial_raw,

    -- Agricultural raw score
    -- NH3(100%)
    (w_nh3 * 1.00) AS agricultural_raw,

    -- Construction raw score
    -- PM2.5(20%) + PM10(25%)
    (w_pm25 * 0.20) + (w_pm10 * 0.25) AS construction_raw,

    -- Residential raw score (biomass burning, stoves, generators)
    -- PM2.5(25%) + CO(30%)
    (w_pm25 * 0.25) + (w_co * 0.30) AS residential_raw,

    -- Natural raw score (windblown dust)
    -- PM10(25%)
    (w_pm10 * 0.25) AS natural_raw

  FROM source_weights
),

normalized AS (
  SELECT
    *,
    -- Sum of all raw category scores (denominator for normalization)
    (vehicular_raw + industrial_raw + agricultural_raw
     + construction_raw + residential_raw + natural_raw) AS category_total
  FROM raw_scores
)

SELECT
  country, state, city, station, last_update_ts, last_update_date, latitude, longitude,
  pm2_5_min, pm2_5_max, pm2_5_avg,
  pm10_min, pm10_max, pm10_avg,
  no2_min, no2_max, no2_avg,
  so2_min, so2_max, so2_avg,
  co_min, co_max, co_avg,
  ozone_min, ozone_max, ozone_avg,
  nh3_min, nh3_max, nh3_avg,
  aqi_status,
  aqi_adverse_effects,

  -- Raw severity-weighted scores (for proper aggregation at city/state level)
  vehicular_raw,
  industrial_raw,
  agricultural_raw,
  construction_raw,
  residential_raw,
  natural_raw,

  -- Source attribution percentages (rounded to 1 decimal)
  CASE WHEN category_total > 0
    THEN ROUND(vehicular_raw    / category_total * 100, 1) ELSE NULL END AS vehicular_pct,
  CASE WHEN category_total > 0
    THEN ROUND(industrial_raw   / category_total * 100, 1) ELSE NULL END AS industrial_pct,
  CASE WHEN category_total > 0
    THEN ROUND(agricultural_raw / category_total * 100, 1) ELSE NULL END AS agricultural_pct,
  CASE WHEN category_total > 0
    THEN ROUND(construction_raw / category_total * 100, 1) ELSE NULL END AS construction_pct,
  CASE WHEN category_total > 0
    THEN ROUND(residential_raw  / category_total * 100, 1) ELSE NULL END AS residential_pct,
  CASE WHEN category_total > 0
    THEN ROUND(natural_raw      / category_total * 100, 1) ELSE NULL END AS natural_pct,

  -- Human-readable summary: top contributors sorted by percentage
  CASE WHEN category_total > 0 THEN
    CONCAT_WS(', ',
      CASE WHEN vehicular_raw    / category_total >= 0.05
        THEN CONCAT('Vehicular ', ROUND(vehicular_raw / category_total * 100, 1), '%') END,
      CASE WHEN industrial_raw   / category_total >= 0.05
        THEN CONCAT('Industrial ', ROUND(industrial_raw / category_total * 100, 1), '%') END,
      CASE WHEN agricultural_raw / category_total >= 0.05
        THEN CONCAT('Agricultural ', ROUND(agricultural_raw / category_total * 100, 1), '%') END,
      CASE WHEN construction_raw / category_total >= 0.05
        THEN CONCAT('Construction ', ROUND(construction_raw / category_total * 100, 1), '%') END,
      CASE WHEN residential_raw  / category_total >= 0.05
        THEN CONCAT('Residential ', ROUND(residential_raw / category_total * 100, 1), '%') END,
      CASE WHEN natural_raw      / category_total >= 0.05
        THEN CONCAT('Natural ', ROUND(natural_raw / category_total * 100, 1), '%') END
    )
  ELSE 'No data available'
  END AS caused_by

FROM normalized;
