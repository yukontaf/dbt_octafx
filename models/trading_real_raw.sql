with
    source as (select * from {{ source("wh_raw", "trading_real_raw") }}),

    renamed as (

        select
            account_created_dt,
            account_created_ut,
            balance,
            balance_at_close,
            balance_operation_type,
            close_price,
            close_time,
            close_time_dt,
            close_time_since_acc_open,
            close_time_since_reg,
            cmd,
            comment,
            commission,
            conv_rate1,
            copy_trade_commission,
            equity_at_close,
            equity_at_open,
            expiration,
            is_closed_by_hedge,
            is_pending,
            leverage_at_open,
            login,
            margin_at_open,
            margin_rate,
            modify_time,
            open_price,
            open_time,
            open_time_dt,
            operation_id,
            pending_created,
            pending_type,
            profit,
            reason,
            registered_dt,
            registered_ut,
            shard,
            spread_close,
            spread_open,
            status,
            swap_free_commission,
            swaps,
            symbol_digits,
            symbol_name,
            trading_account_currency,
            trading_account_id,
            trading_account_leverage,
            type,
            user_id,
            volume,
            initial_open_operation_id,
            initial_open_volume,
            open_reason,
            position_id,
            platform_name,
            open_deal_price

        from source

    )

select *
from renamed
where open_time_dt >= "2024-05-01" and cmd < 2
