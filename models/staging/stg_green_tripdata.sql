select
    -- identifiers
    VendorID as vendor_id,
    RatecodeID as rate_code_id,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    'green' as service_type,
    -- payment
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    payment_type

from {{ source('raw_data', 'green_tripdata') }}