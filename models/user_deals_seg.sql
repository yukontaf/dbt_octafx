{{ config(materialized="view") }}

select
    u.user_id,
    coalesce(t.operation_id, 0) as operation_id,
    coalesce(t.symbol_name, '') as symbol_name,
    coalesce(t.cmd, 0) as cmd,
    coalesce(t.volume, 0) as volume,
    coalesce(t.open_price, 0) as open_price,
    coalesce(t.close_price, 0) as close_price,
    coalesce(t.profit, 0) as profit,
    coalesce(t.open_time_dt, cast('1970-01-01' as timestamp)) as open_time_dt,
    coalesce(t.close_time_dt, cast('1970-01-01' as timestamp)) as close_time_dt,
    coalesce(date(date_trunc(t.open_time_dt, day)), date('1970-01-01')) as trade_day
from {{ ref("users_segment") }} u
left join
    {{ source("wh_raw", "trading_real_raw") }} as t
    on cast(u.user_id as int) = cast(t.user_id as int)
    and (date(t.open_time_dt) between '{{ var(' start ') }}' and '{{ var(' end ') }}')
    and (date(t.close_time_dt) between '{{ var(' start ') }}' and '{{ var(' end ') }}')
    and t.cmd < 2
