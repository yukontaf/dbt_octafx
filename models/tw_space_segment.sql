-- file: models/tw_space_segment.sql

with trading_real_raw as (
    select * from {{ source('wh_raw', 'trading_real_raw') }}
),

events_octa_raw_app_feed_reading as (
    select * from {{ source('amplitude', 'events_octa_raw_app_feed_reading') }}
),

events_octa_raw_web_feed_reading as (
    select * from {{ source('amplitude', 'events_octa_raw_web_feed_reading') }}
)

SELECT distinct t1.user_id
FROM trading_real_raw AS t1
WHERE NOT EXISTS (
        SELECT 1
        FROM events_octa_raw_app_feed_reading AS t2
        WHERE time >= '2024-05-01' and t1.user_id = t2.user_id
    )
and NOT EXISTS (
        SELECT 1
        FROM events_octa_raw_web_feed_reading AS t3
        WHERE
            t1.user_id = t3.user_id and time >= '2024-05-01'
    )
