{{ config(materialized="view") }}

with
    mobile_appsflyer_source as (
        select *
        from {{ source("wh_raw", "mobile_appsflyer") }}
        where event_time_dt >= '2024-01-01'
    ),

    users_segment as (
        with
            user_last_deal as (
                select user_id, max(close_time_dt) as last_deal_time
                from {{ ref("trading_real_raw") }}
                group by user_id
            )

        select
            c.user_id,
            max(
                coalesce(
                    cp.raw_properties.google_push_notification_id is not null, false
                )
            ) as has_token,
            max(coalesce(cc.properties.action = 'accept', false)) as has_consent
        from {{ source("bloomreach", "campaign") }} as c
        left join
            {{ source("bloomreach", "customers_properties") }} as cp
            on c.internal_customer_id = cp.internal_id
        left join
            {{ source("bloomreach", "consent") }} as cc
            on c.internal_customer_id = cc.internal_customer_id
        left join user_last_deal as uld on cast(c.user_id as int) = uld.user_id
        where
            c.properties.status = 'delivered'
            and c.timestamp between '2023-06-01' and current_timestamp()
            and c.user_id is not null
            and uld.last_deal_time
            >= timestamp_sub(current_timestamp(), interval 30 day)
        group by c.user_id
    ),

    user_sessions as (
        select
            customer_user_id,
            min(event_time_dt) as session_start,
            max(event_time_dt) as session_end,
            timestamp_diff(
                max(event_time_dt), min(event_time_dt), second
            ) as session_duration
        from mobile_appsflyer_source
        where event_name = 'session'
        group by customer_user_id
    ),

    users_segment_with_sessions as (
        select us.user_id, avg(usn.session_duration) as avg_session_duration
        from users_segment us
        join user_sessions usn on us.user_id = usn.customer_user_id
        group by us.user_id
    )

select *
from users_segment_with_sessions
