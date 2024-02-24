{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_taxi_trips') }}
  where cast(pickup_datetime as date) between '2019-01-01' and '2019-12-31'
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    {{ dbt.safe_cast("SR_flag", api.Column.translate_type("integer")) }} as SR_flag
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}