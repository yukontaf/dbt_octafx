

  create or replace view `analytics-147612`.`dev_gsokolov`.`int_notreg_user_id`
  OPTIONS()
  as with
first_not_registered as (
    select
        user_id,
        min(cast(timestamp as datetime)) as first_error_timestamp
    from `analytics-147612`.`dev_gsokolov`.`int_notreg_pushes`
    where error = "NotRegistered"
    group by user_id
)

select distinct
    uep.user_id,
    fnr.first_error_timestamp
from `analytics-147612`.`dev_gsokolov`.`int_notreg_pushes` as uep
inner join first_not_registered as fnr on uep.user_id = fnr.user_id
group by 1, 2;

