{{ config(materialized='view') }}

with raw as (
  select
    state,
    cast(left(year, 4) as int64)                   as year_start,
    cast(per_capita_consumption as float64)        as per_capita_consumption
  from
    `dm2-project-458114.dm2_project.historic_per_capita_data`
  where state in (
    'Delhi', 'Maharashtra', 'Tamil Nadu', 'Karnataka', 'Gujarat',
    'Punjab', 'West Bengal', 'Madhya Pradesh', 'Odisha', 'Assam'
  )
)

select * from raw
