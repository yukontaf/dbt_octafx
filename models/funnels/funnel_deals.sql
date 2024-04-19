{{ config(
        materialized='view',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["user_id"]
    ) }}

SELECT
    user_id
    , cmd
    , open_time_dt AS timestamp
    , symbol_name
FROM {{source('wh_raw', 'trading_real_raw')}}
WHERE open_time_dt >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY), MONTH)
    AND open_time_dt < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY) and cmd < 2
    and symbol_name = 'XAUUSD'
GROUP BY user_id, cmd, timestamp, symbol_name