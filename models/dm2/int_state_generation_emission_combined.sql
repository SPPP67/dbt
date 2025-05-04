{{ config(materialized='table') }}

WITH generation AS (
  SELECT
    state,
    EXTRACT(YEAR FROM reading_month) AS year,
    EXTRACT(MONTH FROM reading_month) AS month,
    variable AS energy_source,
    SUM(metric_value) AS total_generation_gwh
  FROM {{ ref('stg_historic_electricity_data_monthly') }}
  WHERE LOWER(category) = 'electricity generation'
    AND LOWER(unit) = 'gwh'
  GROUP BY state, year, month, energy_source
),

emission AS (
  SELECT
    state,
    EXTRACT(YEAR FROM reading_month) AS year,
    EXTRACT(MONTH FROM reading_month) AS month,
    variable AS energy_source,
    SUM(metric_value) AS total_emissions_ktco2
  FROM {{ ref('stg_historic_electricity_data_monthly') }}
  WHERE LOWER(category) LIKE 'power sector%'
    AND LOWER(unit) = 'ktco2'
  GROUP BY state, year, month, energy_source
)

SELECT
  g.state,
  g.year,
  g.month,
  g.energy_source,
  g.total_generation_gwh,
  e.total_emissions_ktco2
FROM generation g
LEFT JOIN emission e
  ON g.state = e.state
  AND g.year = e.year
  AND g.month = e.month
  AND LOWER(g.energy_source) = LOWER(e.energy_source)
