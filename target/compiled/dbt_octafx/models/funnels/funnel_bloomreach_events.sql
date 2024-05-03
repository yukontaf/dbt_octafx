

SELECT
    action_id,
    properties.action_name,
    properties.action_type,
    campaign_id,
    event_number,
    properties.platform,
    properties.status,
    timestamp,
    user_id,
    properties.variant
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY
                    timestamp ASC
            ) AS event_number
        FROM
            `analytics-147612`.`bloomreach_raw`.`campaign`
        WHERE
            timestamp >= '2024-04-01'
            AND properties.action_type IN ('mobile notification', 'email', 'split')
    ) AS bloomreach_events
GROUP BY
    action_id,
    properties.action_name,
    properties.action_type,
    campaign_id,
    event_number,
    properties.platform,
    properties.status,
    timestamp,
    user_id,
    properties.variant