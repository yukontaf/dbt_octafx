

with uninstall_events as (
    select * from `analytics-147612`.`dev_gsokolov`.`appsflyer_uninstall_events_report`
),

users as (
    select * from `analytics-147612`.`dev_gsokolov`.`users_cids_all`
),

joined_data as (
    select
        u.*,
        e.appsflyer_id
    from users as u
    inner join uninstall_events as e
        on u.cid = e.appsflyer_id
)

select *
from joined_data