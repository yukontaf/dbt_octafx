{{ config(materialized="view") }}

with
    user_trading_activity as (
        select
            user_id,
            count(operation_id) as number_of_trades,
            sum(volume) as total_volume,
            sum(profit) as total_profit,
            avg(date_diff(close_time_dt, open_time_dt, day)) as avg_trade_duration_days
        from {{ source("wh_raw", "trading_real_raw") }}
        group by user_id
    ),

    user_communication_quality as (
        select
            user_id,
            count(action_id) as total_communications,
            sum(case when action_type = 'open' then 1 else 0 end) as email_opens,
            sum(case when action_type = 'click' then 1 else 0 end) as link_clicks,
            -- AVG(delta_time) AS avg_response_time,
            count(
                case
                    when action_name in ('webinar_attend', 'link_click')
                    then 1
                    else null
                end
            ) as engagements
        from {{ ref("bloomreach_campaign") }}
        where extract(year from timestamp) = extract(year from current_date())
        group by user_id
    )

select
    t.user_id,
    t.number_of_trades,
    t.total_volume,
    t.total_profit,
    t.avg_trade_duration_days,
    c.total_communications,
    c.email_opens,
    c.link_clicks,
    -- c.avg_response_time,
    c.engagements
from user_trading_activity as t
join user_communication_quality as c on t.user_id = c.user_id
