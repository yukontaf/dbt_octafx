with stats as (
    select
        ab.user_id,
        ab.variant,
        COUNT(distinct d.operation_id) as deals_cnt,
        SUM(d.volume) as symbol_volume,
        SUM(
            case
                when
                    d.symbol_name = '{{ var('symbol_name') }}'
                    then d.volume else
                    0
            end
        )
            as {{ var('symbol_name') }}_vol,
        MIN(
            case
                when
                    d.symbol_name = '{{ var('symbol_name') }}'
                    then d.open_time_dt
            end
        )
            as first_{{ var('symbol_name') }}_deal,
        SUM(
            case
                when d.symbol_name = '{{ var('symbol_name') }}' then 1 else 0
            end
        )
            as {{ var('symbol_name') }}_deals_cnt,
        IF(
            SUM(
                case
                    when d.symbol_name = '{{ var('symbol_name') }}' then 1 else
                        0
                end
            )
            > 0,
            1,
            0
        )
            as {{ var('symbol_name') }}_converted
    from
        {{ ref('ab_users') }} as ab
    left join {{ ref('user_deals') }} as d
        on
            ab.user_id = d.user_id
            and DATE(
                d.close_time_dt
            ) between '{{ var('start') }}' and '{{ var('end') }}'
            and d.variant is not NULL
    group by
        ab.user_id, ab.variant
)

select
    user_id,
    variant,
    deals_cnt,
    PERCENT_RANK()
        over (partition by user_id order by deals_cnt)
        as deals_cnt_percentile,
    symbol_volume,
    PERCENT_RANK()
        over (partition by user_id order by symbol_volume)
        as symbol_volume_percentile,
    {{ var('symbol_name') }}_vol,
    PERCENT_RANK()
        over (partition by user_id order by {{ var('symbol_name') }}_vol)
        as {{ var('symbol_name') }}_vol_percentile,
    first_{{ var('symbol_name') }}_deal,
    {{ var('symbol_name') }}_deals_cnt,
    PERCENT_RANK()
        over (partition by user_id order by {{ var('symbol_name') }}_deals_cnt)
        as {{ var('symbol_name') }}_deals_cnt_percentile,
    {{ var('symbol_name') }}_converted
from stats