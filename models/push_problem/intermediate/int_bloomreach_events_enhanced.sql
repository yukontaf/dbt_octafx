SELECT
    c.*,
    cp.raw_properties.google_push_notification_id,
    u.country_code,
    u.registered_dt
FROM {{ ref('stg_bloomreach_events') }} AS c
INNER JOIN {{ ref('stg_users') }} AS u
    ON safe_cast(c.user_id AS INTEGER) = u.user_id
    AND TIMESTAMP_TRUNC(
        c.timestamp, DAY
    ) BETWEEN u.registered_dt AND u.registered_dt + INTERVAL 28 DAY

LEFT JOIN {{ source('bloomreach', 'customers_properties') }} AS cp
    ON c.internal_customer_id = cp.internal_id
