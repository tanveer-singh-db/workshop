-- 03_calculate_aqi.sql
-- Compute per-pollutant AQI severity using India AQI breakpoints,
-- then derive overall aqi_status (worst pollutant) and aqi_adverse_effects.

CREATE OR REPLACE TABLE aqi_final AS
WITH severity AS (
  SELECT
    *,

    -- PM2.5 severity (µg/m³)
    CASE
      WHEN pm2_5_avg IS NULL THEN NULL
      WHEN pm2_5_avg <= 30  THEN 1
      WHEN pm2_5_avg <= 60  THEN 2
      WHEN pm2_5_avg <= 90  THEN 3
      WHEN pm2_5_avg <= 120 THEN 4
      WHEN pm2_5_avg <= 250 THEN 5
      ELSE 6
    END AS pm2_5_severity,

    -- PM10 severity (µg/m³)
    CASE
      WHEN pm10_avg IS NULL THEN NULL
      WHEN pm10_avg <= 50  THEN 1
      WHEN pm10_avg <= 100 THEN 2
      WHEN pm10_avg <= 250 THEN 3
      WHEN pm10_avg <= 350 THEN 4
      WHEN pm10_avg <= 430 THEN 5
      ELSE 6
    END AS pm10_severity,

    -- NO2 severity (µg/m³)
    CASE
      WHEN no2_avg IS NULL THEN NULL
      WHEN no2_avg <= 40  THEN 1
      WHEN no2_avg <= 80  THEN 2
      WHEN no2_avg <= 180 THEN 3
      WHEN no2_avg <= 280 THEN 4
      WHEN no2_avg <= 400 THEN 5
      ELSE 6
    END AS no2_severity,

    -- SO2 severity (µg/m³)
    CASE
      WHEN so2_avg IS NULL THEN NULL
      WHEN so2_avg <= 40   THEN 1
      WHEN so2_avg <= 80   THEN 2
      WHEN so2_avg <= 380  THEN 3
      WHEN so2_avg <= 800  THEN 4
      WHEN so2_avg <= 1600 THEN 5
      ELSE 6
    END AS so2_severity,

    -- CO severity (mg/m³)
    CASE
      WHEN co_avg IS NULL THEN NULL
      WHEN co_avg <= 1.0 THEN 1
      WHEN co_avg <= 2.0 THEN 2
      WHEN co_avg <= 10  THEN 3
      WHEN co_avg <= 17  THEN 4
      WHEN co_avg <= 34  THEN 5
      ELSE 6
    END AS co_severity,

    -- Ozone severity (µg/m³)
    CASE
      WHEN ozone_avg IS NULL THEN NULL
      WHEN ozone_avg <= 50  THEN 1
      WHEN ozone_avg <= 100 THEN 2
      WHEN ozone_avg <= 168 THEN 3
      WHEN ozone_avg <= 208 THEN 4
      WHEN ozone_avg <= 748 THEN 5
      ELSE 6
    END AS ozone_severity,

    -- NH3 severity (µg/m³)
    CASE
      WHEN nh3_avg IS NULL THEN NULL
      WHEN nh3_avg <= 200  THEN 1
      WHEN nh3_avg <= 400  THEN 2
      WHEN nh3_avg <= 800  THEN 3
      WHEN nh3_avg <= 1200 THEN 4
      WHEN nh3_avg <= 1800 THEN 5
      ELSE 6
    END AS nh3_severity

  FROM aqi_pivoted
),

worst AS (
  SELECT
    *,
    GREATEST(
      COALESCE(pm2_5_severity, 0),
      COALESCE(pm10_severity, 0),
      COALESCE(no2_severity, 0),
      COALESCE(so2_severity, 0),
      COALESCE(co_severity, 0),
      COALESCE(ozone_severity, 0),
      COALESCE(nh3_severity, 0)
    ) AS max_severity
  FROM severity
)

SELECT
  country,
  state,
  city,
  station,
  last_update,
  latitude,
  longitude,
  pm2_5_min, pm2_5_max, pm2_5_avg,
  pm10_min, pm10_max, pm10_avg,
  no2_min, no2_max, no2_avg,
  so2_min, so2_max, so2_avg,
  co_min, co_max, co_avg,
  ozone_min, ozone_max, ozone_avg,
  nh3_min, nh3_max, nh3_avg,

  CASE max_severity
    WHEN 1 THEN 'Good'
    WHEN 2 THEN 'Satisfactory'
    WHEN 3 THEN 'Moderate'
    WHEN 4 THEN 'Poor'
    WHEN 5 THEN 'Very Poor'
    WHEN 6 THEN 'Severe'
    ELSE 'Unknown'
  END AS aqi_status,

  CASE max_severity
    WHEN 1 THEN 'Minimal impact'
    WHEN 2 THEN 'Minor breathing discomfort to sensitive people'
    WHEN 3 THEN 'Breathing discomfort to people with lung/heart disease'
    WHEN 4 THEN 'Breathing discomfort to most people on prolonged exposure'
    WHEN 5 THEN 'Respiratory illness on prolonged exposure'
    WHEN 6 THEN 'Affects healthy people and seriously impacts those with existing diseases'
    ELSE 'Unknown'
  END AS aqi_adverse_effects

FROM worst;
