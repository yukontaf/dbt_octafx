SELECT
  i.appsflyer_id,
  CASE
    WHEN status = 'failed' THEN 1
    WHEN status != 'failed' THEN 0
  END as if_failed,
  e.timestamp,
  e.status,
  e.action_id,
  e.action_type,
  e.campaign_name
FROM
  {{ref('int_bloomreach_events_enhanced')}} as e
  LEFT JOIN {{ref('int_af_id')}} AS i ON e.user_id = i.user_id
WHERE
  (
    e.action_type = 'mobile notification'
  )