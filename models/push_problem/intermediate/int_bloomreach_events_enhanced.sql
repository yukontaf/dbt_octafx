select
    c.*,
    u.country_code,
    u.registered_dt
from {{ ref('stg_bloomreach_events') }} as c
inner join {{ref('stg_users')}} as u
    on
        safe_cast(c.user_id as integer) = u.user_id
        and timestamp_trunc(
            c.timestamp, day
        ) between u.registered_dt and u.registered_dt
        + interval 28 day
