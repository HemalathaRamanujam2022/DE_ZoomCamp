select * from trip_data limit 2;

-- Question 0 
WITH latest_dropoff as (select max(tpep_dropoff_datetime) as latest_DO_time from trip_data)
select taxi_zone_do.Zone as dropoff_zone
from trip_data
join latest_dropoff
on trip_data.tpep_dropoff_datetime =  latest_DO_time
JOIN taxi_zone as taxi_zone_do
ON trip_data.DOLocationID = taxi_zone_do.location_id;

select tpep_dropoff_datetime , tpep_pickup_datetime, 
(tpep_dropoff_datetime - tpep_pickup_datetime) as time_diff,
extract(epoch from tpep_dropoff_datetime - tpep_pickup_datetime) / 60 as time_diff_mins
from trip_data
limit 4;

-- Question 1
CREATE MATERIALIZED VIEW trip_times AS SELECT
        taxi_zone_pu.Zone as pickup_zone,
        taxi_zone_do.Zone as dropoff_zone,
        count(*) as num_trips,
        max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time,
        min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time,
        avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
        max(extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) as max_trip_time_mins,
        min(extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) as min_trip_time_mins,
        avg(extract(epoch from(tpep_dropoff_datetime - tpep_pickup_datetime)) / 60) as avg_trip_time_mins
    FROM
        trip_data
    JOIN taxi_zone as taxi_zone_pu
        ON trip_data.PULocationID = taxi_zone_pu.location_id
    JOIN taxi_zone as taxi_zone_do
        ON trip_data.DOLocationID = taxi_zone_do.location_id
    GROUP BY 1,2;

select * from trip_times
order by avg_trip_time desc
limit 1;

WITH longest_trip as (select max(avg_trip_time) as highest_avg_time from trip_times)
select trip_times.* from trip_times,
longest_trip
where trip_times.avg_trip_time = longest_trip.highest_avg_time;


-- Question 2

CREATE MATERIALIZED VIEW longest_num_trips AS SELECT
        pickup_zone,
        dropoff_zone,
        count(*) as num_of_trips,
        sum(num_trips) as num_trips
 FROM
        trip_times
           where avg_trip_time
    = (select max(avg_trip_time) from trip_times)
    GROUP BY 1,2;

select * from longest_num_trips;
 

WITH longest_trip as (select max(avg_trip_time) as highest_avg_time from trip_times)
select sum(num_trips) from trip_times,
longest_trip
where trip_times.avg_trip_time = longest_trip.highest_avg_time;


--- Question # 3
WITH latest_pickup as (select max(tpep_pickup_datetime) as latest_PU_time 
from trip_data)
SELECT
    taxi_zone.Zone AS pickup_zone,
    count(*) AS last_17_hour_pickup_cnt
FROM
    trip_data
    JOIN latest_pickup
    ON trip_data.tpep_pickup_datetime between (latest_PU_time - INTERVAL '17' HOUR)
    and latest_PU_time
        JOIN taxi_zone
            ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY
    taxi_zone.Zone
ORDER BY last_17_hour_pickup_cnt DESC, pickup_zone desc
    LIMIT 3;


