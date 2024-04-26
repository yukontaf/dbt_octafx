with subq as (SELECT
*
FROM {{ref('int_bloomreach_events_enhanced')}}
WHERE safe_cast(user_id AS int64) IN (
SELECT user_id
FROM {{ref('eligible_users')}}
)
AND action_type = 'mobile notification'
AND status IN ('delivered', 'failed')
)
select user_id, date(timestamp) as timestamp, status, action_type, error
from subq
