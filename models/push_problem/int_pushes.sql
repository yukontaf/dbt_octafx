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

    select
        user_id,
        registered_dt,
        country
    from {{ source('wh_raw', 'users') }}
    where registered_dt >= '2024-01-01'

),

countries as (select * from {{ source('wh_raw', 'countries') }}),

tokens as (
    select
        internal_id,
        raw_properties.google_push_notification_id as token
    from {{ source('bloomreach', 'customers_properties') }}
),

users_countries as (

    select
        user_id,
        registered_dt,
        code
    from users as u
    left join countries as c on u.country = c.country
),

filtered_events as (
    select *
    from {{ ref('int_bloomreach_events_enhanced') }}
    where action_type = 'mobile notification' and status = 'delivered'
),

installed as (
    select
        cast(customer_user_id as unt),
        event_time_dt
    from
        {{ source('wh_raw', 'mobile_appsflyer') }} as a
    inner join
        users_countries as u
        on a.customer_user_id = cast(u.user_id as int)
    where
        event_time_dt >= '2024-01-01'
        and event_type = 'install'
)

select
    uc.user_id,
    t.token
from installed
