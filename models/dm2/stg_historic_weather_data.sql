{{ config(materialized='view') }}

with raw as (
  select
    city,
    datetime_ist                   as reading_time,
    cast(aqi as int64)             as aqi,
    cast(co as float64)            as co,
    cast(`no` as float64)          as no_level,      
    cast(no2 as float64)           as no2,
    cast(o3 as float64)            as o3,
    cast(so2 as float64)           as so2,
    cast(nh3 as float64)           as nh3,
    cast(pm2_5 as float64)         as pm2_5,
    cast(pm10 as float64)          as pm10
  from
    `dm2-project-458114.dm2_project.historic_weather_data`
)

select * from raw
