-- 02_pivot_pollutants.sql
-- Pivot from one-row-per-pollutant to one-row-per-station-timestamp.
-- Each pollutant's min/max/avg becomes its own set of columns.

CREATE OR REPLACE TABLE aqi_pivoted AS
SELECT
  country,
  state,
  city,
  station,
  last_update,
  latitude,
  longitude,

  -- PM2.5
  MAX(CASE WHEN pollutant_id = 'PM2.5' THEN pollutant_min END) AS pm2_5_min,
  MAX(CASE WHEN pollutant_id = 'PM2.5' THEN pollutant_max END) AS pm2_5_max,
  MAX(CASE WHEN pollutant_id = 'PM2.5' THEN pollutant_avg END) AS pm2_5_avg,

  -- PM10
  MAX(CASE WHEN pollutant_id = 'PM10' THEN pollutant_min END) AS pm10_min,
  MAX(CASE WHEN pollutant_id = 'PM10' THEN pollutant_max END) AS pm10_max,
  MAX(CASE WHEN pollutant_id = 'PM10' THEN pollutant_avg END) AS pm10_avg,

  -- NO2
  MAX(CASE WHEN pollutant_id = 'NO2' THEN pollutant_min END) AS no2_min,
  MAX(CASE WHEN pollutant_id = 'NO2' THEN pollutant_max END) AS no2_max,
  MAX(CASE WHEN pollutant_id = 'NO2' THEN pollutant_avg END) AS no2_avg,

  -- SO2
  MAX(CASE WHEN pollutant_id = 'SO2' THEN pollutant_min END) AS so2_min,
  MAX(CASE WHEN pollutant_id = 'SO2' THEN pollutant_max END) AS so2_max,
  MAX(CASE WHEN pollutant_id = 'SO2' THEN pollutant_avg END) AS so2_avg,

  -- CO (mg/m³)
  MAX(CASE WHEN pollutant_id = 'CO' THEN pollutant_min END) AS co_min,
  MAX(CASE WHEN pollutant_id = 'CO' THEN pollutant_max END) AS co_max,
  MAX(CASE WHEN pollutant_id = 'CO' THEN pollutant_avg END) AS co_avg,

  -- Ozone (O3)
  MAX(CASE WHEN pollutant_id = 'OZONE' THEN pollutant_min END) AS ozone_min,
  MAX(CASE WHEN pollutant_id = 'OZONE' THEN pollutant_max END) AS ozone_max,
  MAX(CASE WHEN pollutant_id = 'OZONE' THEN pollutant_avg END) AS ozone_avg,

  -- NH3
  MAX(CASE WHEN pollutant_id = 'NH3' THEN pollutant_min END) AS nh3_min,
  MAX(CASE WHEN pollutant_id = 'NH3' THEN pollutant_max END) AS nh3_max,
  MAX(CASE WHEN pollutant_id = 'NH3' THEN pollutant_avg END) AS nh3_avg

FROM aqi_cleaned
GROUP BY country, state, city, station, last_update, latitude, longitude;
