with
first_not_registered as (
    select
        user_id,
        min(cast(timestamp as datetime)) as first_error_timestamp
    from {{ ref('int_notreg_pushes') }}
    where error = "NotRegistered"
    group by user_id
)

select distinct
    uep.user_id,
    fnr.first_error_timestamp
from {{ ref('int_notreg_pushes') }} as uep
inner join first_not_registered as fnr on uep.user_id = fnr.user_id
group by 1, 2
