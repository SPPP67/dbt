{{ config(materialized='view') }}

with raw as (
  select
    city,
    timestamp                as reading_time,
    cast(lat as float64)     as latitude,
    cast(lon as float64)     as longitude,
    cast(aqi as int64)       as aqi,
    cast(pm2_5 as float64)   as pm2_5,
    cast(pm10 as float64)    as pm10,
    cast(no2 as float64)     as no2,
    cast(so2 as float64)     as so2,
    cast(co as float64)      as co,
    cast(o3 as float64)      as o3
  from
    `dm2-project-458114.dm2_project.real-time-aqi-data`
)

select * from raw
