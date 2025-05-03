{{ config(materialized='table') }}

with base as (

  select
    state,
    city,
    parse_date('%Y-%m', reading_month) as month_date,
    extract(year from parse_date('%Y-%m', reading_month)) as year,
    extract(month from parse_date('%Y-%m', reading_month)) as month,
    aqi,
    co,
    no_level,
    no2,
    o3,
    so2,
    nh3,
    pm2_5,
    pm10
  from {{ ref('int_city_aqi_formatted') }}

),

aggregated as (

  select
    state,
    city,
    year,
    month,
    cast(round(avg(aqi), 2) as float64)      as avg_aqi,
    cast(round(avg(co), 2) as float64)       as avg_co,
    cast(round(avg(no_level), 2) as float64) as avg_no_level,
    cast(round(avg(no2), 2) as float64)      as avg_no2,
    cast(round(avg(o3), 2) as float64)       as avg_o3,
    cast(round(avg(so2), 2) as float64)      as avg_so2,
    cast(round(avg(nh3), 2) as float64)      as avg_nh3,
    cast(round(avg(pm2_5), 2) as float64)    as avg_pm2_5,
    cast(round(avg(pm10), 2) as float64)     as avg_pm10
  from base
  group by state, city, year, month

),

with_band as (

  select
    *,
    case
      when floor(avg_aqi) = 1 then 'Good'
      when floor(avg_aqi) = 2 then 'Fair'
      when floor(avg_aqi) = 3 then 'Moderate'
      when floor(avg_aqi) = 4 then 'Poor'
      when floor(avg_aqi) = 5 then 'Very Poor'
      else 'Unknown'
    end as aqi_category
  from aggregated

)

select * from with_band
