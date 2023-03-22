{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'fhv' as service_type 
    from {{ ref('fhv_stg_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.trip_id,
    fhv_data.service_type,
    fhv_data.dispatching_base_num, 
    fhv_data.PUlocationID, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime, 
    fhv_data.sr_flag, 
    fhv_data.affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOlocationID = dropoff_zone.locationid