{{ config(materialized='table') }}

with formatted as (

  select
    city,
    -- Map city → state
    case
      when city = 'Delhi' then 'Delhi'
      when city = 'Mumbai' then 'Maharashtra'
      when city = 'Chennai' then 'Tamil Nadu'
      when city = 'Bangalore' then 'Karnataka'
      when city = 'Ahmedabad' then 'Gujarat'
      when city = 'Ludhiana' then 'Punjab'
      when city = 'Kolkata' then 'West Bengal'
      when city = 'Bhopal' then 'Madhya Pradesh'
      when city = 'Bhubaneswar' then 'Odisha'
      when city = 'Guwahati' then 'Assam'
    end as state,

    format_date('%Y-%m', cast(reading_time as date)) as reading_month,
    cast(aqi as int64)       as aqi,
    cast(co as float64)      as co,
    cast(no_level as float64) as no_level,
    cast(no2 as float64)     as no2,
    cast(o3 as float64)      as o3,
    cast(so2 as float64)     as so2,
    cast(nh3 as float64)     as nh3,
    cast(pm2_5 as float64)   as pm2_5,
    cast(pm10 as float64)    as pm10

  from {{ ref('stg_historic_weather_data') }}
  where city is not null

)

select * from formatted
