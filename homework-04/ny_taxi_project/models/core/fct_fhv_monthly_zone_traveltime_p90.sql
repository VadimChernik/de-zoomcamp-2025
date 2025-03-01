{{ 
    config(
        materialized='table'
    )
}}

with trips as (
  select 
    *
    ,timestamp_diff(dropoff_datetime,pickup_datetime,second) trip_duration 
  from {{ ref('dim_fhv_trips') }})

,percentiles as (
  select
    year
    ,month
    ,pickup_zone
    ,dropoff_zone
    ,percentile_cont(trip_duration,0.90) over(partition by year,month,pickup_locationid,dropoff_locationid) p90
  from trips)

,final as (
  select distinct
    * 
  from percentiles
  where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
    and year = 2019
    and month = 11)

select 
    * 
from final
where true
qualify row_number() over(partition by pickup_zone order by p90 desc) = 2