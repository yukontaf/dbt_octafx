{{ config(materialized="table") }}

with
    user_last_deal as (
        select user_id, max(open_time_dt) as last_deal_time
        from {{ source("wh_raw", "trading_real_raw") }}
        where cmd < 2
        group by user_id
    )

select
    safe_cast(c.user_id as int64) as user_id,
    c.internal_customer_id,
    max(
        coalesce(cp.raw_properties.google_push_notification_id is not null, false)
    ) as has_token,
    max(coalesce(cc.properties.action = 'accept', false)) as has_consent
from {{ source("bloomreach", "campaign") }}  as c
left join
    {{ source("bloomreach", "customers_properties") }} as cp
    on c.internal_customer_id = cp.internal_id
left join
    {{ source("bloomreach", "consent") }} as cc
    on c.internal_customer_id = cc.internal_customer_id
left join user_last_deal as uld on safe_cast(c.user_id as int64) = uld.user_id
where
    c.user_id is not null
    and uld.last_deal_time >= timestamp_sub(current_timestamp(), interval 45 day)
group by c.user_id, c.internal_customer_id
having
    max(
        coalesce(cp.raw_properties.google_push_notification_id is not null, false)
    ) or
    max(coalesce(cc.properties.action = 'accept', false))
