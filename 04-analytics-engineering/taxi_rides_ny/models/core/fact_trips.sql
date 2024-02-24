{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type,
        null as SR_flag
    from {{ ref('stg_green_taxi_trips') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type,
        null as SR_flag
    from {{ ref('stg_yellow_taxi_trips') }}
), 
fhv_tripdata as (
    select  tripid,
            0 as vendorid,
            0 as ratecodeid,
            pickup_locationid,
            dropoff_locationid,
            pickup_datetime,
            dropoff_datetime,
            '' as store_and_fwd_flag,
            0 as passenger_count,
            0 as trip_distance,
            null as trip_type,
            0 as fare_amount,
            0 as extra,
            0 as mta_tax,
            0 as tip_amount,
            0 as tolls_amount,
            0 as ehail_fee,
            0 as improvement_surcharge,
            0 as total_amount,
            0 as payment_type,
            '' as payment_type_description,
           'Fhv' as service_type,
            SR_flag


    from {{ ref('stg_fhv_taxi_trips') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
    union all 
    select * from fhv_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description,
    trips_unioned.SR_flag
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid