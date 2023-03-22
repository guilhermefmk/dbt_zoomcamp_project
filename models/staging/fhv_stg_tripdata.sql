{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by pickup_datetime) as rn
  from {{ source('staging','external_fhv') }}
  -- where PUlocationID is not null 
)

select 
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num','pickup_datetime']) }} as trip_id,
    cast(dispatching_base_num as string) as dispatching_base_num,

    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(PUlocationID as integer) as PUlocationID,
    cast(DOlocationID as integer) as DOlocationID,
    cast(SR_Flag as numeric) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from tripdata

{% if var('is_test_run', default=false) -%}

    limit 100

{% endif %}