{{ config(
        materialized='view',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["user_id"]
    ) }}

with uninstall_events as (
    select * from {{ ref('appsflyer_uninstall_events_report') }}
),

users as (
    select * from {{ ref('users_cids_all') }}
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
