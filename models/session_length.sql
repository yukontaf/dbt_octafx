with
    event_data as (
        select
            customer_user_id,
            event_time_dt,
            lag(event_time_dt) over (
                partition by customer_user_id order by event_time_dt
            ) as prev_event_time
        from {{ ref("mobile_appsflyer") }}
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
select customer_user_id, avg(session_length) as avg_session_length
from session_lengths
group by customer_user_id
