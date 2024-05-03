SELECT
    cast(user_id AS numeric) AS user_id,
    client_id AS appsflyer_id,
    af.install_time_dt
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY user_id ORDER BY dt DESC) AS row_num
    FROM `analytics-147612`.`wh_raw`.`users_cids_all`
    WHERE
        user_id IN (
            SELECT DISTINCT user_id
            FROM `analytics-147612`.`dev_gsokolov`.`int_bloomreach_events_enhanced`
        )
) AS s
INNER JOIN `analytics-147612`.`wh_raw`.`mobile_appsflyer` AS af
    ON
        s.client_id = af.appsflyer_id
        AND af.event_time_dt >= '2024-01-01'
WHERE s.row_num = 1