

  create or replace view `analytics-147612`.`dev_gsokolov`.`stg_pushes`
  OPTIONS()
  as select ue.*
from `analytics-147612`.`dev_gsokolov`.`stg_bloomreach_events` as ue
where
    action_type = 'mobile notification'
    and status in ('delivered', 'clicked', 'failed');

