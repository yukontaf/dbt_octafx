{{ config(
        materialized='view',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["user_id"]
    ) }}

SELECT
    internal_customer_id,
    SAFE_CAST(user_id AS INT64) as user_id,
    timestamp,
    campaign_id,
    action_id,
    type,
    properties.campaign_name,
    properties.status,
    properties.error,
    properties.action_name,
    properties.action_type,
    properties.variant,
    properties.platform,
    properties.consent_category,
    raw_properties.campaign_policy,
    CONCAT(campaign_id, '_', CAST(action_id AS STRING)) AS event_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS event_number,
    CASE
        WHEN properties.status = 'sent'
        THEN TIMESTAMP_DIFF(LEAD(timestamp, 1) OVER (PARTITION BY user_id, CONCAT(campaign_id, '_', CAST(action_id AS STRING)) ORDER BY timestamp), timestamp, SECOND)
        WHEN properties.status = 'delivered'
        THEN TIMESTAMP_DIFF(timestamp, LAG(timestamp, 1) OVER (PARTITION BY user_id, CONCAT(campaign_id, '_', CAST(action_id AS STRING)) ORDER BY timestamp), SECOND)
        ELSE NULL
    END AS time_to_delivered,
    CASE
        WHEN properties.status = 'sent'
        THEN TIMESTAMP_DIFF(LEAD(timestamp, 1) OVER (PARTITION BY user_id, CONCAT(campaign_id, '_', CAST(action_id AS STRING)) ORDER BY timestamp), timestamp, SECOND)
        WHEN properties.status = 'failed'
        THEN TIMESTAMP_DIFF(timestamp, LAG(timestamp, 1) OVER (PARTITION BY user_id, CONCAT(campaign_id, '_', CAST(action_id AS STRING)) ORDER BY timestamp), SECOND)
        ELSE NULL
    END AS time_to_failure
FROM
    bloomreach_raw.campaign
WHERE
    timestamp >= '2024-01-01'
