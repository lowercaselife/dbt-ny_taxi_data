with green_tripdata as (
    select * from {{ ref('stg_green_tripdata') }}
),

yellow_tripdata as (
    select * from {{ ref("stg_yellow_tripdata") }}
),

trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),

trips_deduped as (
    select *,
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime, service_type
            order by pickup_datetime
        ) as rn
    from trips_unioned
    where pickup_datetime is not null
      and dropoff_datetime is not null
)

select * from trips_deduped
where rn = 1