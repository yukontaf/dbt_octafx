select
    c.*,
    u.country_code,
    u.registered_dt
from {{ ref('stg_bloomreach_events') }} as c
inner join {{ref('stg_users')}} as u
    on
        safe_cast(c.user_id as int64) = u.user_id
        and timestamp_trunc(
            c.event_tstmp, day
        ) between u.registered_dt and u.registered_dt
        + interval 28 day
