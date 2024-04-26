with uninstall_events as (
    select * from {{ ref('appsflyer_uninstall_events_report') }}
),

users as (
    select * from {{ ref('users_cids_all') }}
),

joined_data as (
    select u.*, e.appsflyer_id
    from users u
    join uninstall_events e
    on u.client_id = e.appsflyer_id
)

select *
from joined_data