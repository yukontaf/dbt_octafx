select
    u.*,
    af.appsflyer_id,
    af.install_time_dt
from `analytics-147612`.`dev_gsokolov`.`stg_users` as u
inner join
    `analytics-147612`.`dev_gsokolov`.`int_af_id` as af
    on u.user_id = af.user_id