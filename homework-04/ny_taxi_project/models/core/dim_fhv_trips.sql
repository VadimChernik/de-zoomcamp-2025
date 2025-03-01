{{ 
    config(
        materialized='table'
    )
}}

with trips as (
  select 
    * 
  from {{ ref('stg_fhv_tripdata') }})

,dim_zones as (
  select 
    * 
  from {{ ref('dim_zones') }}
  where borough != 'Unknown')

select 
  t.*,

  extract(year from pickup_datetime) year,
  extract(quarter from pickup_datetime) quarter,
  extract(month from pickup_datetime) month,

  pz.borough as pickup_borough, 
  pz.zone as pickup_zone, 
  dz.borough as dropoff_borough, 
  dz.zone as dropoff_zone
  
from trips t
  inner join dim_zones as pz
    on t.pickup_locationid = pz.locationid
  inner join dim_zones as dz
    on t.dropoff_locationid = dz.locationid