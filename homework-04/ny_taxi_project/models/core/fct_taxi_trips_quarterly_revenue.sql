{{ 
    config(
        materialized='table'
    )
}}

with revenue as (
  select
    service_type
    ,extract(year from pickup_datetime) year
    ,extract(quarter from pickup_datetime) quarter
    ,sum(total_amount) revenue
  from {{ ref('fact_trips') }}
  where extract(year from pickup_datetime) in (2019,2020)
  group by all)

,year_over_year as (
  select 
    r.*
    ,lag(r.revenue) over(partition by r.service_type,r.quarter order by r.year) revenue_last_year
  from revenue r)

,dates as (
  select distinct 
    year
    ,quarter
    ,year_quarter 
  from {{ ref('dim_dates') }})

select 
  service_type
  ,d.year_quarter
  ,round((revenue/revenue_last_year-1)*100,2) yoy_pct
from year_over_year y
  left join dates d
    on y.year = d.year
    and y.quarter = d.quarter
where y.year = 2020