with
    source as (select * from {{ source("wh_raw", "deposits_enhanced") }}),

    renamed as (

        select
            user_id,
            deposit_id,
            deposit_type,
            local_id,
            mtaccount_id,
            billing_account_id,
            account,
            amount,
            amount_usd,
            created,
            response_created,
            registered,
            time_since_registration,
            time_since_account_opening,
            from_account,
            from_paysystem_id,
            referrer_mtaccount_id,
            currency,
            credited_amount,
            credited_amount_currency,
            bonus_id,
            bonus_type,
            bonus_status,
            bonus_amount,
            bonus_currency,
            bonus_size,
            country,
            country_code,
            platform,
            leverage,
            account_type,
            source,
            mobile_app,
            payment_transaction_id,
            created_dt,
            response_created_dt,
            id,
            deposit_bracket,
            deposit_number,
            time_since_last_deposit

        from source

    )

select *
from renamed
where extract(year from created_dt) = extract(year from current_date())
