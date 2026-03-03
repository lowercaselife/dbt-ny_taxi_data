with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

trips_deduped as (
    select *,
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime, service_type
            order by pickup_datetime
        ) as rn
    from trips
    where pickup_datetime is not null
      and dropoff_datetime is not null
),

final as (
    select
        -- generate trip_id here instead
        to_hex(md5(concat(
            cast(vendor_id as string), '-',
            cast(pickup_datetime as string), '-',
            cast(dropoff_datetime as string), '-',
            service_type
        ))) as trip_id,

        -- service
        service_type,

        -- identifiers
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,

        -- timestamps
        pickup_datetime,
        dropoff_datetime,

        -- trip info
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,

        -- payment
        t.payment_type,
        p.description as payment_type_description,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount

    from trips_deduped t
    left join payment_types p
        on t.payment_type = p.payment_type
    where rn = 1        -- deduplicate here
)

select * from final