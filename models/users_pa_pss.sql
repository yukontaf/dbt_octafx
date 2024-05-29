-- Step 1: Fetch users who registered this year and filled personal info within 3 days
-- after registration
{# inactive_filled_personal_info as (
    -- Assuming the inactive_filled_personal_info.sql script is included here
    select * from {{ ref('inactive_filled_personal_info') }}
), #}
-- Step 2: Fetch payment system select events
with
    payment_system_select_source as (
        select *
        from {{ source("amplitude", "events_octa_raw_deposit_payment_system_select") }}
    ),

    payment_system_selects as (
        select user_id, time as payment_system_select_time
        from payment_system_select_source
    ),

    -- Step 3: Join registered users with payment system select events
    users_with_payment_system_select as (
        select i.user_id, i.registered_dt, p.payment_system_select_time
        from {{ ref("inactive_filled_personal_info") }} i
        inner join payment_system_selects p on i.user_id = p.user_id
        where
            p.payment_system_select_time >= i.registered_dt + interval 4 day
            and p.payment_system_select_time <= i.registered_dt + interval 7 day
    ),

    -- Step 4: Filter out users who have not performed a payment system select within
    -- the desired time frame
    users_with_payment_system_select_within_timeframe as (
        select user_id, registered_dt
        from users_with_payment_system_select
        group by user_id, registered_dt
        having count(payment_system_select_time) >= 1
    )

-- Final step: Select from the joined result
select *
from users_with_payment_system_select_within_timeframe
