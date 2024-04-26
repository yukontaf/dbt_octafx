{{ config(
        materialized='view',
        clustering=["appsflyer_id"]
    ) }}

SELECT
    a.appsflyer_id,
    a.action_id,
    a.campaign_name,
    MIN(a.timestamp) AS attempt_time,
    MIN(f.timestamp) AS failure_time,
    TIMESTAMP_DIFF(MIN(a.timestamp), MIN(f.timestamp), SECOND)
      AS time_to_failure_seconds
FROM
    {{ref('timedelta_helper')}} AS a
INNER JOIN
    {{ref('timedelta_helper')}} AS f
ON a.appsflyer_id = f.appsflyer_id
    AND a.action_id = f.action_id
    AND f.status = 'failed'
WHERE
    a.action_type = 'mobile notification'
    AND a.status = 'sent'
    AND f.timestamp > a.timestamp
GROUP BY
    a.appsflyer_id, a.action_id, a.campaign_name

