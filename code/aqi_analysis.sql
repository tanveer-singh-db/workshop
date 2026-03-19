-- aqi_analysis.sql
-- Sample analytical queries on the aqi_enriched table.
-- These are standalone SELECT queries (not CREATE TABLE) for ad-hoc analysis.

-- ============================================================
-- 1. Emission contributors by state
--    Sums raw severity-weighted scores across all stations in
--    each state, then normalizes to percentages.
-- ============================================================
SELECT
  state,
  COUNT(*) AS station_count,
  ROUND(SUM(vehicular_raw)    / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS vehicular_pct,
  ROUND(SUM(industrial_raw)   / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS industrial_pct,
  ROUND(SUM(agricultural_raw) / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS agricultural_pct,
  ROUND(SUM(construction_raw) / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS construction_pct,
  ROUND(SUM(residential_raw)  / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS residential_pct,
  ROUND(SUM(natural_raw)      / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS natural_pct
FROM aqi_enriched
WHERE vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw > 0
GROUP BY state
ORDER BY station_count DESC;

-- ============================================================
-- 2. Emission contributors by city
--    Same approach at city granularity.
-- ============================================================
SELECT
  state,
  city,
  COUNT(*) AS station_count,
  ROUND(SUM(vehicular_raw)    / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS vehicular_pct,
  ROUND(SUM(industrial_raw)   / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS industrial_pct,
  ROUND(SUM(agricultural_raw) / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS agricultural_pct,
  ROUND(SUM(construction_raw) / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS construction_pct,
  ROUND(SUM(residential_raw)  / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS residential_pct,
  ROUND(SUM(natural_raw)      / SUM(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw) * 100, 1) AS natural_pct
FROM aqi_enriched
WHERE vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw > 0
GROUP BY state, city
ORDER BY station_count DESC;

-- ============================================================
-- 3. Top polluted cities by AQI status
--    Counts stations in each AQI category per city.
-- ============================================================
SELECT
  state,
  city,
  aqi_status,
  COUNT(*) AS station_count
FROM aqi_enriched
GROUP BY state, city, aqi_status
ORDER BY
  CASE aqi_status
    WHEN 'Severe'            THEN 1
    WHEN 'Very Poor'         THEN 2
    WHEN 'Poor'              THEN 3
    WHEN 'Moderately Polluted' THEN 4
    WHEN 'Satisfactory'      THEN 5
    WHEN 'Good'              THEN 6
    ELSE 7
  END,
  station_count DESC;

-- ============================================================
-- 4. Worst states ranked by average severity
--    Uses average raw score total as a proxy for overall
--    pollution severity.
-- ============================================================
SELECT
  state,
  COUNT(*) AS station_count,
  ROUND(AVG(vehicular_raw + industrial_raw + agricultural_raw + construction_raw + residential_raw + natural_raw), 2) AS avg_severity_score,
  ROUND(AVG(vehicular_raw), 2)    AS avg_vehicular,
  ROUND(AVG(industrial_raw), 2)   AS avg_industrial,
  ROUND(AVG(agricultural_raw), 2) AS avg_agricultural,
  ROUND(AVG(construction_raw), 2) AS avg_construction,
  ROUND(AVG(residential_raw), 2)  AS avg_residential,
  ROUND(AVG(natural_raw), 2)      AS avg_natural
FROM aqi_enriched
GROUP BY state
ORDER BY avg_severity_score DESC;
