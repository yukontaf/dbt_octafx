with
    -- Step 1: Fetch users who registered this year and did not perform a deposit
    -- within 3 days after registration
    users_3d as (select u.user_id, u.registered_dt from {{ ref("users_3d") }} u),

    -- Step 2: Fetch payment system selection events
    payment_system_select as (
        select *
        from {{ source("amplitude", "events_octa_raw_deposit_payment_system_select") }}
    ),

    -- Step 3: Find users who performed payment system selection between 4 and 7 days
    -- after registration
    payment_system_action_within_4_to_7_days as (
        select ps.user_id, ps.time as payment_selection_time
        from payment_system_select ps
        join users_3d u on ps.user_id = u.user_id
        where
            ps.time
            between u.registered_dt
            + interval 4 day and u.registered_dt
            + interval 7 day
        group by ps.user_id, ps.time
    )

-- Final step: Select the relevant user ids
select distinct user_id
from payment_system_action_within_4_to_7_days
