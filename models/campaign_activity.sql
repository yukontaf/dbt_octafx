{{ config(
    materialized='view'
) }}

WITH
user_activity AS (
    SELECT
        b.user_id,
        COUNT(DISTINCT action_id) AS total_actions,
        COUNT(DISTINCT action_type) AS action_variety,
        COUNT(DISTINCT campaign_id) AS campaigns_engaged,
        MAX(timestamp) AS last_activity_time,
        MIN(timestamp) AS first_activity_time,
        DATE_DIFF(MAX(timestamp), MIN(timestamp), DAY) AS activity_span_days,
        -- AVG(delta_time) AS avg_response_time,
        MAX(attempts) AS max_attempts,
        ARRAY_AGG(DISTINCT device) AS devices_used,
        ARRAY_AGG(DISTINCT os) AS os_used,
        ARRAY_AGG(DISTINCT browser) AS browsers_used,
        ARRAY_AGG(DISTINCT platform) AS platforms_used
    FROM {{ref('bloomreach_campaign')}} b
    inner join {{ref('users_segment')}} u
    on cast(b.user_id as int) = cast(u.user_id as int)
    WHERE extract(year FROM timestamp) = extract(year FROM current_date())
    GROUP BY user_id
)

SELECT
    ua.user_id,
    ua.total_actions,
    ua.action_variety,
    ua.campaigns_engaged,
    ua.last_activity_time,
    ua.first_activity_time,
    ua.activity_span_days,
    -- ua.avg_response_time,
    ua.max_attempts,
    ARRAY_TO_STRING(ua.devices_used, ',') AS devices_used,
    ARRAY_TO_STRING(ua.os_used, ',') AS os_used,
    ARRAY_TO_STRING(ua.browsers_used, ',') AS browsers_used,
    ARRAY_TO_STRING(ua.platforms_used, ',') AS platforms_used
FROM user_activity AS ua
limit 100000
