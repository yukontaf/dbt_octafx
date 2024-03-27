with
events as (
    select
        *,
        row_number() over (
            partition by user_id order by cast(event_tstmp as datetime)
        ) as event_number
    from {{ ref('int_bloomreach_events_enhanced') }}
    where
        user_id in (select distinct user_id from {{ ref('int_notreg_user_id') }})
),

first_not_registered as (
    select
        user_id,
        min(cast(event_tstmp as datetime)) as first_error_timestamp,
        min(event_number) as first_error_event_number
    from events
    where error = "NotRegistered"
    group by user_id
)

select
    e.user_id,
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
    event_tstmp,
    type,
    fnr.first_error_timestamp,
    fnr.first_error_event_number
from events as e
left join first_not_registered as fnr on e.user_id = fnr.user_id
