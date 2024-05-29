{{ config(materialized="incremental", unique_key="user_id") }}

with
    source as (select * from {{ ref("trading_real_raw") }}), filter as (
        select * from source where open_time_dt >= '2024-05-01' and cmd < 2
    ),

    aggregated as (
        select
            user_id,
            trading_account_id,
            count(operation_id) as total_operations,
            sum(profit) as total_profit,
            avg(balance) as avg_balance,
            sum(volume) as total_volume,
            avg(volume) as avg_volume_per_trade,
            count(distinct symbol_name) as distinct_symbols,
            -- Calculate segments, like Premium/Loyal customers, based on business logic
            case
                when avg(balance) > 10000
                then 'Premium'
                when count(distinct symbol_name) > 10
                then 'Loyal'
                else 'Regular'
            end as segment

        from filter
        group by user_id, trading_account_id
    )

select *
from aggregated
