with u as (
    select distinct user_id
    from wh_raw.users
    where EXTRACT(year from registered_dt) = 2024
),

b as (
    select distinct SAFE_CAST(user_id as int64) as user_id
    from bloomreach_raw.campaign where
        DATE(timestamp) between '2024-01-01' and DATE(CURRENT_TIMESTAMP)
        and SAFE_CAST(user_id as int64) in (select user_id from u)
        and properties.action_type = 'mobile notification'
        and properties.status = 'delivered'
)

select * from b
