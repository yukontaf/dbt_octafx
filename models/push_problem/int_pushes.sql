   SELECT 
      *,
      CASE 
        WHEN event_order = 1 THEN timestamp 
        ELSE NULL 
      END AS first_event 
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY timestamp
        ) AS event_order 
      FROM {{ref('int_bloomreach_events_enhanced')}}
      WHERE safe_cast(user_id as int64) IN (
        SELECT user_id 
        FROM {{ref('eligible_users')}}
      )
      AND action_type = 'mobile notification'
      AND date(timestamp) BETWEEN '2024-01-01' AND date(current_timestamp)
      AND status IN ('delivered', 'failed')
    )