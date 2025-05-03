{{ config(materialized='table') }}

-- Optional soft ref to include upstream lineage explicitly
-- {% do ref('stg_live_aqi_data') %}

select
  city,
  state,
  reading_date,
  current_aqi_index,
  historic_aqi_index,
  aqi_status
from {{ ref('int_aqi_live_vs_historic') }}
