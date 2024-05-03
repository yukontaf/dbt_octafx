with
pushes as (
    select
        *,
        row_number() over (
            partition by user_id order by cast(timestamp as datetime)
        ) as event_number
    from `analytics-147612`.`dev_gsokolov`.`int_bloomreach_events_enhanced`
    where
        user_id in (select distinct user_id from `analytics-147612`.`dev_gsokolov`.`int_notreg_user_id`
        )
        and (
            action_type = 'mobile notification'
            and status in ('delivered', 'clicked', 'failed')
        )
),

first_not_registered as (
    select
        user_id,
        min(cast(timestamp as datetime)) as first_error_timestamp,
        min(event_number) as first_error_event_number
    from pushes
    where error = 'NotRegistered'
    group by user_id
),

first_error_event as (
    select
        user_id,
        min(cast(timestamp as datetime)) as first_error_timestamp
    from
        pushes
    where
        error = 'NotRegistered'
    group by
        user_id
),

next_delivered_event as (
    select
        e.user_id,
        min(cast(e.timestamp as datetime)) as next_delivered_timestamp
    from
        pushes as e
    inner join
        first_error_event as fee
        on e.user_id = fee.user_id
    where
        e.status = 'delivered'
        and cast(e.timestamp as datetime) > fee.first_error_timestamp
    group by
        e.user_id
)

select
    fee.user_id,
    fee.first_error_timestamp,
    nde.next_delivered_timestamp,
    date_diff(nde.next_delivered_timestamp, fee.first_error_timestamp, second)
        as time_difference_in_seconds
from
    first_error_event as fee
left join
    next_delivered_event as nde
    on fee.user_id = nde.user_id