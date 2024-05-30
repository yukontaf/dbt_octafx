{{ config(materialized="view") }}

with
    user_last_deal as (
        select user_id, max(close_time_dt) as last_deal_time
        from {{ source("wh_raw", "trading_real_raw") }} as tr
        group by user_id
    ),

    user_segment as (
        select
            cast(c.user_id as int) as user_id,
            max(coalesce(cp.raw_properties.google_push_notification_id is not null, false)) as has_token,
            max(coalesce(cc.properties.action = 'accept', false)) as has_consent
        from {{ source("bloomreach", "campaign") }} as c
        left join
            {{ source("bloomreach", "customers_properties") }} as cp
            on c.internal_customer_id = cp.internal_id
        left join
            {{ source("bloomreach", "consent") }} as cc
            on c.internal_customer_id = cc.internal_customer_id
        where
            c.internal_customer_id is not null
            and c.user_id is not null
        group by c.user_id
    )
    

select
    us.user_id,
    us.has_token,
    us.has_consent
from user_segment as us
inner join user_last_deal as uld on us.user_id = uld.user_id
where
    (us.has_token or us.has_consent)
    and uld.last_deal_time >= timestamp_sub(current_timestamp(), interval 45 day)
