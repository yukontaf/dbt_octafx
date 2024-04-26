SELECT
  campaign_name,
  date(attempt_time) as day,
  AVG(time_to_failure_seconds) AS avg_time_delta_seconds
FROM
  {{ ref('timedelta_per_user') }}
  {{ dbt_utils.group_by(n=2) }}