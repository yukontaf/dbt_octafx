with
    -- Step 1: Base source table
    source as (select * from {{ source("wh_raw", "users") }}),

    -- Step 2: Rename columns appropriately
    renamed as (
        select
            user_id,
            registered_ut,
            registered_dt,
            verification_status,
            is_email_verified,
            is_locked,
            lock_comment,
            last_login,
            birthdate,
            country_code,
            country,
            city,
            deposited_total,
            withdrawn_total,
            reg_info,
            referrer_id,
            is_unsubscribed,
            language,
            language_id,
            support_language_id,
            is_withdraw_disabled,
            is_deposit_disabled,
            email_domain,
            email_hash,
            email_hash_md5,
            firstname_hash,
            lastname_hash,
            phone_hash,
            phone_filled
        from source
    ),

    -- Step 3: User segment
    user_last_deal as (
        select user_id, max(close_time_dt) as last_deal_time
        from {{ ref("trading_real_raw") }}
        group by user_id
    ),

    users_segment as (
        select
            cast(c.user_id as int) as user_id,
            max(
                coalesce(
                    cp.raw_properties.google_push_notification_id is not null, false
                )
            ) as has_token,
            max(coalesce(cc.properties.action = 'accept', false)) as has_consent
        from {{ source("bloomreach", "campaign") }} as c
        left join
            {{ source("bloomreach", "customers_properties") }} as cp
            on c.internal_customer_id = cp.internal_id
        left join
            {{ source("bloomreach", "consent") }} as cc
            on c.internal_customer_id = cc.internal_customer_id
        left join user_last_deal as uld on cast(c.user_id as int) = uld.user_id
        where
            c.properties.status = 'delivered'
            and c.timestamp between '2023-06-01' and current_timestamp()
            and c.user_id is not null
            and uld.last_deal_time
            >= timestamp_sub(current_timestamp(), interval 30 day)
        group by c.user_id
    ),

    -- Step 4: Trading data
    trading_data as (
        select user_id, count(*) as deal_count, sum(volume) as total_volume
        from {{ source("wh_raw", "trading_real_raw") }}
        where
            open_time_dt >= '2024-01-01'  -- Only consider deals made this year
            and cmd < 2
        group by user_id
    ),

    -- Step 5: Users segment trading
    users_segment_trading as (
        select us.user_id, us.has_token, us.has_consent, td.deal_count, td.total_volume
        from users_segment us
        left join user_last_deal as uld on cast(us.user_id as int) = uld.user_id
        left join trading_data as td on cast(us.user_id as int) = td.user_id
        where uld.last_deal_time >= timestamp_sub(current_timestamp(), interval 30 day)
        group by
            us.user_id, us.has_token, us.has_consent, td.deal_count, td.total_volume
    ),

    -- Step 6: Combine and filter data
    final_users as (
        select r.*
        from renamed r
        join users_segment_trading ust on cast(r.user_id as int) = ust.user_id
    )

-- Step 7: Select final desired output
select *
from final_users f
join users_segment_trading ust using (user_id)
