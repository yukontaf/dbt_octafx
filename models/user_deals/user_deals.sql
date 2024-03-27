SELECT
    t.user_id
    , u.variant
    , operation_id
    , symbol_name
    , cmd
    , volume
    , open_price
    , close_price
    , profit
    , open_time_dt
    , close_time_dt
    , DATE(DATE_TRUNC(open_time_dt, DAY)) AS trade_day
FROM
    {{ source("wh_raw", "trading_real_raw") }} AS t
    INNER JOIN {{ ref('ab_users') }} AS u
        ON t.user_id = CAST(u.user_id AS INT)
WHERE
    DATE(close_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}'
    AND cmd < 2