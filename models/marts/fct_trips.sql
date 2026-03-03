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

-- flag duplicates
trips_ranked as (
    select *,
        row_number() over (
            partition by trip_id, service_type
            order by pickup_datetime
        ) as rn
    from trips_unioned
    where trip_id is not null
),

-- keep only first occurrence
deduplicated as (
    select * from trips_ranked
    where rn = 1
)

select * from deduplicated