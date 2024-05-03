

SELECT
    internal_customer_id
    , SAFE_CAST(user_id AS INT64) as user_id
    , timestamp
    , campaign_id
    , action_id
    , type
    , properties.campaign_name
    , properties.status
    , properties.error
    , properties.action_name
    , properties.action_type
    , properties.variant
    , properties.platform
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS event_number
FROM bloomreach_raw.campaign
WHERE timestamp >= '2024-01-01'