select
    c.*,
    u.country_code,
    u.registered_dt
from `analytics-147612`.`dev_gsokolov`.`stg_bloomreach_events` as c
inner join `analytics-147612`.`dev_gsokolov`.`stg_users` as u
    on
        safe_cast(c.user_id as integer) = u.user_id
        and timestamp_trunc(
            c.timestamp, day
        ) between u.registered_dt and u.registered_dt
        + interval 28 day