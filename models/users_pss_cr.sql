with
    -- Step 1: Fetch users who registered this year and did not perform a deposit
    -- within 3 days after registration
    users_3d as (select u.user_id, u.registered_dt from {{ ref("users_3d") }} u),

    -- Step 2: Fetch payment system selection events and rename the table for easier
    -- usage
    payment_system_select as (
        select pss.user_id, pss.time
        from
            {{ source("amplitude", "events_octa_raw_deposit_payment_system_select") }} pss
    ),

    -- Step 3: Mark users from users_3d who performed a payment system selection
    -- between 4 and 7 days after registration
    user_payment_system_action as (
        select
            u.user_id,
            case
                when
                    exists (
                        select 1
                        from payment_system_select p
                        where
                            p.user_id = u.user_id
                            and p.time
                            between u.registered_dt
                            + interval 4 day and u.registered_dt
                            + interval 7 day
                    )
                then 1
                else 0
            end as performed_payment_system_select
        from users_3d u
    )

-- Step 4: Calculate conversion statistics
select
    count(*) as total_users,
    sum(performed_payment_system_select) as converted_users,
    (sum(performed_payment_system_select) * 100.0 / count(*)) as conversion_rate
from user_payment_system_action
