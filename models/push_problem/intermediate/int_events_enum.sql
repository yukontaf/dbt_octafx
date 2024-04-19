with
events as (
    select
        *,
        row_number() over (
            partition by user_id order by cast(timestamp as datetime)
        ) as event_number
    from {{ ref('int_bloomreach_events_enhanced') }}
    where
        user_id in (select distinct user_id from {{ ref('int_notreg_user_id') }})
        and status in ('delivered', 'clicked', 'failed')
),

first_not_registered as (
    select
        user_id,
        min(cast(timestamp as datetime)) as first_error_timestamp,
        min(event_number) as first_error_event_number
    from events
    where error = "NotRegistered"
    group by user_id
)

select
    cast(e.user_id as numeric) as user_id,
    country_code,
    registered_dt,
    status,
    campaign_id,
    consent_category,
    sent_timestamp,
    action_name,
    action_id,
    campaign_name,
    platform,
    campaign_trigger,
    action_type,
    ingest_timestamp,
    timestamp,
    type,
    fnr.first_error_timestamp,
    cast(fnr.first_error_event_number as numeric) as first_error_event_number
from events as e
left join first_not_registered as fnr on e.user_id = fnr.user_id

