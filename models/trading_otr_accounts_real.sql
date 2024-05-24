{{ config(materialized="view") }}

with
    source as (select * from {{ source("wh_raw", "trading_otr_accounts_real") }}),

    renamed as (

        select
            id,
            user_id,
            balance,
            bonus,
            margin,
            leverage,
            group_name,
            currency,
            readonly,
            deleted,
            created,
            shard

        from source

    )

select *
from renamed
