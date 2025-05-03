{{ config(materialized='table') }}

with cleaned as (

  select
    city,

    -- City → State Mapping (same as historical AQI)
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

    -- Use timestamp as-is (already IST)
    reading_time as reading_timestamp,
    date(reading_time) as reading_date,
    format_datetime('%T', reading_time) as reading_time_ist,

    -- AQI value
    cast(aqi as float64) as aqi

  from {{ ref('stg_live_aqi_data') }}
  where city is not null

),

with_band as (

  select *,
    case
      when aqi <= 50 then 1
      when aqi <= 100 then 2
      when aqi <= 150 then 3
      when aqi <= 200 then 4
      else 5
    end as aqi_index,

    case
      when aqi <= 50 then 'Good'
      when aqi <= 100 then 'Fair'
      when aqi <= 150 then 'Moderate'
      when aqi <= 200 then 'Poor'
      else 'Very Poor'
    end as aqi_category

  from cleaned

)

select * from with_band
