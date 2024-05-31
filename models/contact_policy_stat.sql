-- models/m_user_events_per_month.sql
{{ config(materialized="view") }}

with
    user_events_per_month as (

        with
            trading_users as (select distinct user_id from {{ ref("trading_users") }}),

            -- Select events within the last 365 days from the current date and
            -- calculate event count per user per month
            recent_events as (
                select
                    user_id,
                    extract(month from timestamp) as event_month,
                    count(*) as event_count
                from {{ source("bloomreach", "campaign") }}
                where timestamp_sub(current_timestamp(), interval 365 day) <= timestamp
                group by user_id, extract(month from timestamp)
            )

        -- Join the distinct users with their recent events per month
        select tu.user_id, re.event_month, re.event_count
        from trading_users tu
        left join recent_events re on tu.user_id = safe_cast(re.user_id as int64)
    )
select *
from user_events_per_month