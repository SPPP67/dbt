{{ config(materialized='view') }}

with raw as (
  select
    country,
    country_code,
    state,
    state_code,
    state_type,
    cast(date as date)              as reading_month,
    category,
    subcategory,
    variable,
    unit,
    cast(value as float64)          as metric_value
  from
    `dm2-project-458114.dm2_project.historic_electricity_Data`
  where state in (
    'Delhi', 'Maharashtra', 'Tamil Nadu', 'Karnataka', 'Gujarat',
    'Punjab', 'West Bengal', 'Madhya Pradesh', 'Odisha', 'Assam'
  )
)

select * from raw
