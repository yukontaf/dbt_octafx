SELECT
    u.user_id,
    u.variant,
    COALESCE(t.operation_id, 0) AS operation_id,
    COALESCE(t.symbol_name, '') AS symbol_name,
    COALESCE(t.cmd, 0) AS cmd,
    COALESCE(t.volume, 0) AS volume,
    COALESCE(t.open_price, 0) AS open_price,
    COALESCE(t.close_price, 0) AS close_price,
    COALESCE(t.profit, 0) AS profit,
    COALESCE(t.open_time_dt, CAST('1970-01-01' AS TIMESTAMP)) AS open_time_dt,
    COALESCE(t.close_time_dt, CAST('1970-01-01' AS TIMESTAMP)) AS close_time_dt,
    COALESCE(DATE(DATE_TRUNC(t.open_time_dt, DAY)), DATE('1970-01-01'))
        AS trade_day
FROM
    `analytics-147612`.`dev_gsokolov`.`ab_users` AS u
LEFT JOIN `analytics-147612`.`wh_raw`.`trading_real_raw`
    AS t ON u.user_id = t.user_id
AND (
    DATE(t.open_time_dt) BETWEEN '2024-04-02' AND '2024-04-09'
)
AND (
    DATE(t.close_time_dt) BETWEEN '2024-04-02' AND '2024-04-09'
)
AND t.cmd < 2