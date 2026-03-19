-- 01_clean_and_load.sql
-- Load raw AQI CSV data, clean and cast to proper types.
-- Handles: 'NA' → NULL, date parsing, whitespace trimming, numeric casting.

CREATE OR REPLACE TABLE aqi_cleaned AS
SELECT
  country,
  state,
  city,
  station,
  to_date(last_update, 'dd-MM-yyyy HH:mm:ss') as last_update_date,
  TO_UTC_TIMESTAMP(TO_TIMESTAMP(last_update, 'dd-MM-yyyy HH:mm:ss'), 'Asia/Kolkata') AS last_update_ts,
  try_cast(TRIM(latitude) as double)  AS latitude,
  try_cast(TRIM(longitude) as double) AS longitude,
  pollutant_id,
  CAST(NULLIF(pollutant_min, 'NA') AS DOUBLE) AS pollutant_min,
  CAST(NULLIF(pollutant_max, 'NA') AS DOUBLE) AS pollutant_max,
  CAST(NULLIF(pollutant_avg, 'NA') AS DOUBLE) AS pollutant_avg
FROM raw_aqi_data;



