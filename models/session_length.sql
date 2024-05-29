with
    event_data as (
        select
            customer_user_id,
            event_time_dt,
            lag(event_time_dt) over (
                partition by customer_user_id order by event_time_dt
            ) as prev_event_time
        from {{ source("wh_raw", "mobile_appsflyer") }}
        where
            extract(year from event_time_dt) = extract(year from current_date())
            and customer_user_id
            in (select distinct user_id from {{ ref("users_segment") }})
    ),
    session_flags as (
        select
            customer_user_id,
            event_time_dt,
            case
                when event_time_dt - prev_event_time > interval 30 minute then 1 else 0
            end as new_session
        from event_data
    ),
    session_data as (
        select
            customer_user_id,
            event_time_dt,
            sum(new_session) over (
                partition by customer_user_id order by event_time_dt
            ) as session_id
        from session_flags
    ),
    session_lengths as (
        select
            customer_user_id,
            session_id,
            max(event_time_dt) - min(event_time_dt) as session_length
        from session_data
        group by customer_user_id, session_id
    )
select *
from session_lengths
