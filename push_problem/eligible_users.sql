SELECT
    u.user_id,
    u.country,
    m.app_type
FROM {{source('wh_raw', 'users'}} AS u
LEFT left joind
    {{source('wh_raw', 'deposits_enhanced'}} AS d
    ON u.user_id = d.user_id
LEFT JOIN
    {{source('wh_raw', 'users_appsflyer_id'}} AS a
    ON u.user_id = a.user_id
LEFT JOIN
    {{source('wh_raw', 'mobile_appsflyer'}} AS f
    ON a.appsflyer_id = f.appsflyer_id
LEFT JOIN
    {{source('wh_raw', 'mobile_applications'}} AS m
    ON f.app_id = m.app_id
WHERE
    u.registered_dt <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND u.country IN ('India', 'Indonesia')
    AND d.user_id IS NULL
    AND a.user_id IS NOT NULL
    AND m.app_type = 'general_app'
