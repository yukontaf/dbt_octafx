{{ config(materialized="view") }}

with
    user_last_deal as (
        select user_id, max(close_time_dt) as last_deal_time
        from {{ ref('trading_real_raw') }}
        group by user_id
    )

select
    c.user_id,
    max(
        coalesce(cp.raw_properties.google_push_notification_id is not null, false)
    ) as has_token,
    max(coalesce(cc.properties.action = 'accept', false)) as has_consent
from `analytics-147612.bloomreach_raw.campaign` as c
left join
    `analytics-147612.bloomreach_raw.customers_properties` as cp
    on c.internal_customer_id = cp.internal_id
left join
    `analytics-147612.bloomreach_raw.consent` as cc
    on c.internal_customer_id = cc.internal_customer_id
left join user_last_deal as uld on cast(c.user_id as int) = uld.user_id
where
    c.properties.status = 'delivered'
    and c.timestamp between '2023-09-01' and '2024-03-31'
    and c.user_id is not null
    and uld.last_deal_time >= timestamp_sub(current_timestamp(), interval 30 day)
group by c.user_id
