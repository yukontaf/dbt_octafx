select ue.*
from {{ ref('stg_bloomreach_events') }} as ue
where action_type = 'mobile notification'