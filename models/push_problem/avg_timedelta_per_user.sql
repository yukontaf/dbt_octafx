{{ config(
        materialized='table',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["appsflyer_id", "campaign_name"]
    ) }}

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
),
TimeDeltas AS (
  SELECT
    f.appsflyer_id,
    TIMESTAMP_DIFF(f.failed_timestamp, a.first_attempt_timestamp, SECOND) AS time_delta_seconds
  FROM
    FailedEvents f
    JOIN FirstAttemptEvents a ON f.appsflyer_id = a.appsflyer_id
)
SELECT
  AVG(time_delta_seconds) AS avg_time_delta_seconds
FROM
  TimeDeltas