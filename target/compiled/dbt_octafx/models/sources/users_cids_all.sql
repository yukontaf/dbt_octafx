

with source as (

    select * from `analytics-147612`.`wh_raw`.`users_cids_all`

),

renamed as (

    select
        client_id,
        user_id,
        dt,
        src_info

    from source

)

select * from renamed