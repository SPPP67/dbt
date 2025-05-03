
{{ config(materialized='table') }}

-- soft dependency for lineage visibility
-- {% do ref('stg_historic_weather_data') %}

with aqi as (
  select *
  from {{ ref('int_city_aqi_monthly') }}
),

electricity as (
  select *
  from {{ ref('int_state_electricity_summary') }}
),

per_capita as (
  select *
  from {{ ref('stg_historic_per_capita') }}
)

select
  aqi.state,
  aqi.year,
  aqi.month,

  -- new: season label based on month
  case
    when aqi.month in (12, 1, 2, 3) then 'Winter'
    when aqi.month in (4, 5, 6)     then 'Summer'
    when aqi.month in (7, 8, 9)     then 'Monsoon'
    when aqi.month in (10, 11)      then 'Post-Monsoon'
  end as season,

  aqi.city,
  aqi.avg_aqi,
  aqi.aqi_category,

  -- pollutants
  aqi.avg_pm2_5,
  aqi.avg_pm10,
  aqi.avg_no2,
  aqi.avg_so2,
  aqi.avg_o3,
  aqi.avg_co,

  -- electricity stats
  electricity.total_generation_gwh,
  electricity.total_emissions_tco2,
  electricity.total_capacity_mw,

  -- per capita
  per_capita.per_capita_consumption

from aqi
left join electricity
  on aqi.state = electricity.state
  and aqi.year = electricity.year
  and aqi.month = electricity.month

left join per_capita
  on aqi.state = per_capita.state  
  and aqi.year = per_capita.year_start 
