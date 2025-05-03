{{ config(materialized='table') }}

with live as (
  select
    city,
    state,
    reading_date,
    reading_time_ist,
    extract(month from reading_date) as reading_month,
    aqi_index as current_aqi_index,
    aqi_category as current_aqi_category
  from {{ ref('int_real_time_aqi_data_cleaned') }}
),

historic as (
  select
    city,
    state,
    month,
    avg_aqi as avg_aqi_index,
    floor(avg_aqi) as historic_aqi_index,
    case
      when floor(avg_aqi) = 1 then 'Good'
      when floor(avg_aqi) = 2 then 'Fair'
      when floor(avg_aqi) = 3 then 'Moderate'
      when floor(avg_aqi) = 4 then 'Poor'
      else 'Very Poor'
    end as historic_aqi_category
  from {{ ref('int_city_aqi_monthly') }}
)

select
  live.city,
  live.state,
  live.reading_date,
  live.reading_time_ist,
  live.reading_month,

  -- Live AQI info
  live.current_aqi_index,
  live.current_aqi_category,

  -- Historic AQI info
  historic.avg_aqi_index,
  historic.historic_aqi_index,
  historic.historic_aqi_category,

  -- Status
  case
    when historic.historic_aqi_index is null then 'No baseline'
    when live.current_aqi_index > historic.historic_aqi_index then 'Worse'
    when live.current_aqi_index < historic.historic_aqi_index then 'Improved'
    else 'Same'
  end as aqi_status

from live
left join historic
  on live.city = historic.city
  and live.reading_month = historic.month
