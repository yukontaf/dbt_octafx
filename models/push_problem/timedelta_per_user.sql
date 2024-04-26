{{ config(
        materialized='view',
        clustering=["appsflyer_id"]
    ) }}

<<<<<<< HEAD
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

=======
WITH RankedEvents AS (
  SELECT
    i.appsflyer_id,
    e.timestamp,
    e.status,
    e.action_id,
    e.action_type,
    e.campaign_name,
    ROW_NUMBER() OVER (PARTITION BY i.appsflyer_id ORDER BY e.timestamp ASC) AS rank
  FROM
    {{ref('int_bloomreach_events_enhanced')}} as e
    LEFT JOIN {{ref('int_af_id')}} AS i ON e.user_id = i.user_id
  WHERE
    e.action_type = 'mobile notification'
),
FailedEvents AS (
  SELECT
    appsflyer_id,
    timestamp AS failed_timestamp
  FROM RankedEvents
  WHERE status = 'failed'
),
FirstAttemptEvents AS (
  SELECT
    appsflyer_id,
    timestamp AS first_attempt_timestamp
  FROM RankedEvents
  WHERE rank = 1
)
SELECT
  f.appsflyer_id,
  f.failed_timestamp,
  a.first_attempt_timestamp,
  TIMESTAMP_DIFF(f.failed_timestamp, a.first_attempt_timestamp, SECOND) AS time_delta_seconds
FROM
  FailedEvents f
  JOIN FirstAttemptEvents a ON f.appsflyer_id = a.appsflyer_id
>>>>>>> 73a18c624c1af89324f02e69ccff0e8ce7396327
