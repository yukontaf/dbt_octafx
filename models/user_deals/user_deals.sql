SELECT
    u.user_id,
    u.variant,
    operation_id,
    symbol_name,
    cmd,
    volume,
    open_price,
    close_price,
    profit,
    open_time_dt,
    close_time_dt,
    DATE(DATE_TRUNC(open_time_dt, DAY)) AS trade_day
FROM
    {{ ref('ab_users') }} u
LEFT JOIN  {{ source("wh_raw", "trading_real_raw") }} AS t
    ON u.user_id = t.user_id
WHERE
    (DATE(open_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}')
    AND (DATE(close_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}')
    AND cmd < 2
