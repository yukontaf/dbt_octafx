with
    source as (select * from {{ source("wh_raw", "trading_otr_deals_real") }}),

    renamed as (

        select
            id,
            order_id,
            position_id,
            account_id,
            symbol,
            direction,
            volume,
            price,
            profit,
            quote_timestamp,
            create_timestamp,
            shard

        from source

    )

select *
from renamed
where create_timestamp >= "2024-05-01"
