SELECT DISTINCT SAFE_CAST(c.user_id AS INT64) AS user_id
FROM
    bloomreach_raw.campaign AS c
INNER JOIN {{ source('wh_raw', 'users') }} AS u
    ON
        c.user_id = CAST(u.user_id AS STRING)
        AND u.registered_dt >= '2024-01-01'
WHERE
    properties.action_type = 'mobile notification'
    AND properties.status = 'delivered'
    AND timestamp >= '2024-01-01'
