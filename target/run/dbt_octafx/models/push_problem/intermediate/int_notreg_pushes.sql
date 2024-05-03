

  create or replace view `analytics-147612`.`dev_gsokolov`.`int_notreg_pushes`
  OPTIONS()
  as select *
from `analytics-147612`.`dev_gsokolov`.`stg_pushes`
where status = 'failed' and error = 'NotRegistered';

