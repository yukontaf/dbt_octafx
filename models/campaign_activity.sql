{{ config(materialized="view") }}

with
    user_activity as (
        select
            b.user_id,
            count(distinct action_id) as total_actions,
            count(distinct action_type) as action_variety,
            count(distinct campaign_id) as campaigns_engaged,
            max(timestamp) as last_activity_time,
            min(timestamp) as first_activity_time,
            date_diff(max(timestamp), min(timestamp), day) as activity_span_days,
            -- AVG(delta_time) AS avg_response_time,
            max(attempts) as max_attempts,
            array_agg(distinct device) as devices_used,
            array_agg(distinct os) as os_used,
            array_agg(distinct browser) as browsers_used,
            array_agg(distinct platform) as platforms_used
        from {{ ref("bloomreach_campaign") }} b
        inner join
            {{ ref("users_segment") }} u
            on cast(b.user_id as int) = cast(u.user_id as int)
        where extract(year from timestamp) = extract(year from current_date())
        group by user_id
    )

select
    ua.user_id,
    ua.total_actions,
    ua.action_variety,
    ua.campaigns_engaged,
    ua.last_activity_time,
    ua.first_activity_time,
    ua.activity_span_days,
    -- ua.avg_response_time,
    ua.max_attempts,
    array_to_string(ua.devices_used, ',') as devices_used,
    array_to_string(ua.os_used, ',') as os_used,
    array_to_string(ua.browsers_used, ',') as browsers_used,
    array_to_string(ua.platforms_used, ',') as platforms_used
from user_activity as ua
limit 100000
