select
    appsflyer_id,
    platform,
    app_version,
    os_version,
    u.user_id
from wh_raw.mobile_appsflyer as a
inner join wh_raw.users_cids_all as u
    on
        a.appsflyer_id = u.client_id
        and u.dt >= '2024-01-01'
where
    install_time_dt >= '2024-01-01'
    and event_type = 'install'
    and event_time_dt >= '2024-01-01'
