with
    user_sessions as (
        select
            sl.customer_user_id,
            {# extract(epoch from avg(sl.session_length)) as avg_session_length_seconds #}
            avg(sl.session_length) as avg_session_length_seconds
        from {{ ref("session_length") }} sl
        group by sl.customer_user_id
    ),

    users_segment as (select * from {{ ref("users_segment") }})

select us.user_id, usg.avg_session_length_seconds
from users_segment us
left join user_sessions usg on us.user_id = usg.customer_user_id
