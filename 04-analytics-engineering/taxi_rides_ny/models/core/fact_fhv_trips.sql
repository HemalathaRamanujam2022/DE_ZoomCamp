{{
    config(
        materialized='table'
    )
}}

select fhv.*,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,   
from {{ ref("stg_fhv_taxi_trips")}} fhv
inner join {{ ref("dim_zones") }} as pickup_zone
on fhv.pickup_locationid = pickup_zone.locationid
inner join {{ ref("dim_zones") }}  as dropoff_zone
on fhv.dropoff_locationid = dropoff_zone.locationid