{{ 
    config(
        materialized='table'
    )
}}

with fare as (
  select
    service_type
    ,extract(year from pickup_datetime) year
    ,extract(month from pickup_datetime) month
    ,fare_amount
  from {{ ref('fact_trips') }}
  where fare_amount > 0 
    and trip_distance > 0 
    and payment_type_description in ('Cash', 'Credit card'))

,percentiles as (
  select 
    service_type
    ,year
    ,month
    ,percentile_cont(fare_amount,0.90 RESPECT NULLS) over(partition by service_type,year,month) p90
    ,percentile_cont(fare_amount,0.95 RESPECT NULLS) over(partition by service_type,year,month) p95
    ,percentile_cont(fare_amount,0.97 RESPECT NULLS) over(partition by service_type,year,month) p97
  from fare)

select distinct 
  * 
from percentiles
where year = 2020 
  and month = 4