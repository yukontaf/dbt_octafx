-- file: models/tw_space_segment.sql
with
    trading_real_raw as (select * from {{ ref("trading_real_raw") }}),

    events_octa_raw_app_feed_reading as (
        select * from {{ ref("feed_app_reading_base") }}
    ),

    events_octa_raw_web_feed_reading as (
        select * from {{ ref("feed_web_reading_base") }}
    )

select distinct t1.user_id
from trading_real_raw as t1
where
    not exists (
        select 1
        from events_octa_raw_app_feed_reading as t2
        where time >= '2024-05-01' and t1.user_id = t2.user_id
    )
    and not exists (
        select 1
        from events_octa_raw_web_feed_reading as t3
        where t1.user_id = t3.user_id and time >= '2024-05-01'
    )
