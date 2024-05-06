SELECT
    a.platform,
    a.os_version,
    a.device_model,
    e.user_id,
    u.client_id
FROM analytics-147612.dev_gsokolov.stg_bloomreach_events AS e
INNER JOIN
    analytics-147612.wh_raw.users_cids_all AS u
    ON CAST(e.user_id AS STRING) = CAST(u.user_id AS STRING)
INNER JOIN
    analytics-147612.wh_raw.mobile_devices_info AS a
    ON u.client_id = a.appsflyer_id
where a.event_time_dt >= '2024-01-01'
group by 1,2,3,4,5