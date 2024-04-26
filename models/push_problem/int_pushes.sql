{{
    config(
        materialized="view",
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day",
        },
        clustering=["user_id"],
    )
}}

with
    users as (

        select user_id, registered_dt, country
        from {{ source('wh_raw', 'users') }}
        where registered_dt >= '2024-01-01'

    ),

    countries as (select * from {{ source('wh_raw', 'countries') }}),
    
    tokens as (
        select internal_id, raw_properties.google_push_notification_id as token
        from {{source('bloomreach', 'customers_properties')}}
    ),

    users_countries as (

        select user_id, registered_dt, code
        from users u
        left join countries c on u.country = c.country
    ),

    filtered_events as (
        select *
        from `dev_gsokolov.stg_bloomreach_events`
        where action_type = 'mobile notification' and status = 'delivered'
    ),

    installed as (
        select customer_user_id, event_time_dt from 
        {{source('wh_raw', 'mobile_appsflyer')}}
        where event_time_dt >= '2024-01-01'
        AND event_type = 'install'
    ),

    temp as (
        select
            uc.user_id,
            t.token,
            b.* except (user_id)
        from users_countries uc
        inner join filtered_events f on uc.user_id = safe_cast(f.user_id as int)
        left join
            {{ref('stg_bloomreach_events')}} b
            on uc.user_id = safe_cast(b.user_id as int)
        inner join installed i on uc.user_id = i.customer_user_id
        left join tokens t on b.internal_customer_id = t.internal_id

    )

select *
from temp
where action_type = 'mobile notification'
