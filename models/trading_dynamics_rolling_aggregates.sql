{{
    config(
        materialized="incremental", unique_key="user_id, period_start, symbol_name"
    )
}}

with
    source as (select * from {{ ref("trading_real_raw") }}), 

    filter as (
        select user_id, operation_id, trading_account_id, symbol_name, open_time_dt, profit, volume
        from source
        where open_time_dt >= '2024-05-01' and cmd < 2
    ),

    period_data as (
        select
            user_id,
            trading_account_id,
            symbol_name,
            date_trunc(open_time_dt, week) as period_start,  -- Change this to 'week' or 'month' for different periods
            sum(profit) as period_profit,
            sum(volume) as period_volume,
            count(operation_id) as period_operations
        from filter
        group by
            user_id, trading_account_id, symbol_name, date_trunc(open_time_dt, week)
    ),

    rolling_aggregates as (
        select
            user_id,
            trading_account_id,
            symbol_name,
            period_start,
            sum(period_profit) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
                rows between unbounded preceding and current row
            ) as cumulative_profit,
            sum(period_volume) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
                rows between unbounded preceding and current row
            ) as cumulative_volume,
            avg(period_profit) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
                rows between unbounded preceding and current row
            ) as avg_profit_per_period,
            avg(period_volume) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
                rows between unbounded preceding and current row
            ) as avg_volume_per_period,
            sum(period_operations) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
                rows between unbounded preceding and current row
            ) as cumulative_operations,
            period_profit,
            period_volume,
            lag(period_profit) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
            ) as prev_period_profit,
            lag(period_volume) over (
                partition by user_id, trading_account_id, symbol_name
                order by period_start
            ) as prev_period_volume
        from period_data
    )

select
    *,
    case
        when prev_period_profit is null or prev_period_profit = 0
        then null
        else (period_profit - prev_period_profit) / prev_period_profit
    end as profit_change_pct,
    case
        when prev_period_volume is null or prev_period_volume = 0
        then null
        else (period_volume - prev_period_volume) / prev_period_volume
    end as volume_change_pct
from rolling_aggregates
