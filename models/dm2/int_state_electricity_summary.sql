{{ config(materialized='table') }}

with filtered as (

  select
    state,
    extract(year from reading_month) as year,
    extract(month from reading_month) as month,
    lower(category) as category,
    lower(subcategory) as subcategory,
    metric_value
  from {{ ref('stg_historic_electricity_data_monthly') }}

),

aggregated as (

  select
    state,
    year,
    month,

    -- Total Generation (in GWh)
    coalesce(sum(case 
      when category = 'electricity generation' and subcategory = 'total' 
      then metric_value 
    end), 0) as total_generation_gwh,

    -- Total Emissions (in tCO2)
    coalesce(sum(case 
      when category = 'power sector emissions' and subcategory = 'total' 
      then metric_value 
    end), 0) as total_emissions_tco2,

    -- Total Installed Capacity (in MW)
    coalesce(sum(case 
      when category = 'capacity' and subcategory = 'aggregate fuel' 
      then metric_value 
    end), 0) as total_capacity_mw

  from filtered
  group by state, year, month

)

select * from aggregated
