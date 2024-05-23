{{ config(
    materialized='view'
) }}

SELECT
    u.user_id,
    COALESCE(t.operation_id, 0) AS operation_id,
    COALESCE(t.symbol_name, '') AS symbol_name,
    COALESCE(t.cmd, 0) AS cmd,
    COALESCE(t.volume, 0) AS volume,
    COALESCE(t.open_price, 0) AS open_price,
    COALESCE(t.close_price, 0) AS close_price,
    COALESCE(t.profit, 0) AS profit,
    COALESCE(t.open_time_dt, CAST('1970-01-01' AS TIMESTAMP)) AS open_time_dt,
    COALESCE(t.close_time_dt, CAST('1970-01-01' AS TIMESTAMP)) AS close_time_dt,
    COALESCE(DATE(DATE_TRUNC(t.open_time_dt, DAY)), DATE('1970-01-01')) AS trade_day
FROM
    {{ ref('users_segment') }} u
    LEFT JOIN {{ source("wh_raw", "trading_real_raw") }} AS t ON cast(u.user_id as int)= cast(t.user_id as int)
        AND (DATE(t.open_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}')
        AND (DATE(t.close_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}')
        AND t.cmd < 2
