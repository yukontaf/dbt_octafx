with
    -- Step 1: Fetch users who registered this year
    user_source as (select * from {{ source("wh_raw", "users") }}),

    registered_users as (
        select user_id, registered_dt, registered_ut
        from user_source
        where extract(year from registered_dt) = extract(year from current_date())
    ),

    -- Step 2: Fetch deposits made this year
    deposit_source as (select * from {{ source("wh_raw", "deposits_enhanced") }}),

    deposits_this_year as (
        select user_id, deposit_id, deposit_type, created_dt
        from deposit_source
        where extract(year from created_dt) = extract(year from current_date())
    ),

    -- Step 3: Filter out users who did not perform a deposit within 3 days after
    -- registration
    users_no_deposit_within_3_days as (
        select u.user_id, u.registered_dt
        from registered_users u
        left join
            deposits_this_year d
            on u.user_id = d.user_id
            and d.created_dt <= u.registered_dt + interval 3 day
        where d.deposit_id is null
    ),

    -- Step 4: Fetch users who filled their personal info
    personal_info_source as (
        select * from {{ source("amplitude", "events_octa_raw_filled_personal_info") }}
    ),

    users_filled_personal_info as (
        select user_id, min(timestamp) as first_personal_info_filled_at
        from personal_info_source
        group by user_id
    ),

    -- Step 5: Join registered users with personal info filled within 3 days after
    -- registration
    users_registered_and_filled as (
        select u.user_id, u.registered_dt
        from users_no_deposit_within_3_days u
        inner join users_filled_personal_info p on u.user_id = p.user_id
        where
            p.first_personal_info_filled_at >= u.registered_dt
            and p.first_personal_info_filled_at <= u.registered_dt + interval 3 day
    )

-- Final step: Select from the joined result
select *
from users_registered_and_filled
