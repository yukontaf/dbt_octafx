{{ config(
        materialized='view',
        partition_by={
            "field": "dt",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["user_id", "client_id"]
    ) }}

with source as (

    select * from {{ source('wh_raw', 'users_cids_all') }}

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

