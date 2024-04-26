SELECT
    cast(user_id as numeric) as user_id
    , client_id AS appsflyer_id
    , af.install_time_dt
FROM (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) AS row_num
    FROM {{source('wh_raw', 'users_cids_all')}}
    WHERE user_id IN (SELECT DISTINCT user_id FROM {{ref('int_bloomreach_events_enhanced')}})
) AS s
    INNER JOIN {{source('wh_raw', 'mobile_appsflyer')}} AS af
        ON s.client_id = af.appsflyer_id
            AND af.event_time_dt >= '2024-01-01'
WHERE s.row_num = 1

