{{ config(
    materialized='view'
) }}

WITH user_trading_activity AS (
    SELECT
        user_id,
        COUNT(operation_id) AS number_of_trades,
        SUM(volume) AS total_volume,
        SUM(profit) AS total_profit,
        AVG(DATE_DIFF(close_time_dt, open_time_dt, DAY)) AS avg_trade_duration_days
    FROM {{source('wh_raw', 'trading_real_raw')}}
    GROUP BY user_id
),

user_communication_quality AS (
    SELECT
        user_id,
        COUNT(action_id) AS total_communications,
        SUM(CASE WHEN action_type = 'open' THEN 1 ELSE 0 END) AS email_opens,
        SUM(CASE WHEN action_type = 'click' THEN 1 ELSE 0 END) AS link_clicks,
        -- AVG(delta_time) AS avg_response_time,
        COUNT(CASE WHEN action_name IN ('webinar_attend', 'link_click') THEN 1 ELSE NULL END) AS engagements
    FROM {{ref('bloomreach_campaign')}}
    WHERE extract(year FROM timestamp) = extract(year FROM current_date())
    GROUP BY user_id
)

SELECT
    t.user_id,
    t.number_of_trades,
    t.total_volume,
    t.total_profit,
    t.avg_trade_duration_days,
    c.total_communications,
    c.email_opens,
    c.link_clicks,
    -- c.avg_response_time,
    c.engagements
FROM user_trading_activity AS t
JOIN user_communication_quality AS c
ON t.user_id = c.user_id
