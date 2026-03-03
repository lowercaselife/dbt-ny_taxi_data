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
        to_hex(md5(concat(
            coalesce(cast(trips_deduped.vendor_id as string), ''), '-',
            coalesce(cast(trips_deduped.pickup_datetime as string), ''), '-',
            coalesce(cast(trips_deduped.dropoff_datetime as string), ''), '-',
            coalesce(trips_deduped.service_type, '')
        ))) as trip_id,

        trips_deduped.service_type,
        trips_deduped.vendor_id,
        trips_deduped.rate_code_id,
        trips_deduped.pickup_location_id,
        trips_deduped.dropoff_location_id,
        trips_deduped.pickup_datetime,
        trips_deduped.dropoff_datetime,
        trips_deduped.store_and_fwd_flag,
        trips_deduped.passenger_count,
        trips_deduped.trip_distance,
        trips_deduped.trip_type,
        trips_deduped.payment_type,
        p.description as payment_type_description,
        trips_deduped.fare_amount,
        trips_deduped.extra,
        trips_deduped.mta_tax,
        trips_deduped.tip_amount,
        trips_deduped.tolls_amount,
        trips_deduped.ehail_fee,
        trips_deduped.improvement_surcharge,
        trips_deduped.total_amount

    from trips_deduped
    left join payment_types p
        on cast(trips_deduped.payment_type as int) = cast(p.payment_type as int)
    where rn = 1
)

select * from final