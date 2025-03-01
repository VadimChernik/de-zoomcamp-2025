{{ 
    config(
        materialized='table'
    )
}}

with dates as (
  select distinct
    extract(year from pickup_datetime) year
    ,extract(quarter from pickup_datetime) quarter
    ,extract(month from pickup_datetime) month
  from {{ ref('fact_trips') }})

select 
  *
  ,year || 'Q' || quarter year_quarter
  ,year || '-' || format('%02d',month) year_month
from dates
