with
    source as (select * from {{ source("wh_raw", "users") }}),

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

    )

select *
from renamed
