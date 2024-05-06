SELECT
    *,
    LEAD(timestamp)
        OVER (
            PARTITION BY user_id, campaign_id, action_id ORDER BY timestamp ASC
        )
        AS next_timestamp
FROM (
    SELECT
        user_id,
        campaign_id,
        campaign_name,
        action_id,
        timestamp,
        status,
        event_id,
        EXTRACT(HOUR FROM timestamp) AS hour,
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    user_id, CONCAT(campaign_id, '_', CAST(action_id AS STRING))
                ORDER BY timestamp
            )
            AS event_number,
        CASE WHEN event_number = 1 THEN timestamp END AS first_push
    FROM `dev_gsokolov.stg_bloomreach_events`
    WHERE
        action_type = 'mobile notification'
        AND DATE(timestamp) BETWEEN '2024-01-01' AND CURRENT_DATE()
        AND status IN ('delivered', 'failed')
) AS detailed_events e
inner join {{ref('also_user_device')}} d
on e.user_id = d.user_id