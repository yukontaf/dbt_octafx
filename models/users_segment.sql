{{ config(
    materialized='view'
) }}

WITH user_last_deal AS (
    SELECT
        user_id,
        MAX(close_time_dt) AS last_deal_time
    FROM `analytics-147612.wh_raw.trading_real_raw`
    GROUP BY user_id
)

SELECT
    c.user_id,
    MAX(COALESCE(cp.raw_properties.google_push_notification_id IS NOT NULL, FALSE)) AS has_token,
    MAX(COALESCE(cc.properties.action = 'accept', FALSE)) AS has_consent
FROM `analytics-147612.bloomreach_raw.campaign` AS c
LEFT JOIN `analytics-147612.bloomreach_raw.customers_properties` AS cp
    ON c.internal_customer_id = cp.internal_id
LEFT JOIN `analytics-147612.bloomreach_raw.consent` AS cc
    ON c.internal_customer_id = cc.internal_customer_id
LEFT JOIN user_last_deal AS uld
    ON cast(c.user_id as int) = uld.user_id
WHERE
    c.properties.status = 'delivered'
    AND c.timestamp BETWEEN '2023-09-01' AND '2024-03-31'
    AND c.user_id IS NOT NULL
    AND uld.last_deal_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY c.user_id
