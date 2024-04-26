with
    stats as (
        select
            user_id,
            variant,
            count(distinct operation_id) as deals_cnt,
            coalesce(sum(volume), 0) as symbol_volume,
            coalesce(
                sum(
                    case
                        when symbol_name = '{{ var(' symbol_name ') }}'
                        then volume
                        else 0
                    end
                ),
                0
            ) as {{ var("symbol_name") }}_vol,
            min(
                case
                    when symbol_name = '{{ var(' symbol_name ') }}' then open_time_dt
                end
            ) as first_{{ var("symbol_name") }}_deal,
            coalesce(
                sum(
                    case
                        when symbol_name = '{{ var(' symbol_name ') }}' then 1 else 0
                    end
                ),
                0
            ) as {{ var("symbol_name") }}_deals_cnt,
            if(
                coalesce(
                    sum(
                        case
                            when symbol_name = '{{ var(' symbol_name ') }}'
                            then 1
                            else 0
                        end
                    ),
                    0
                )
                > 0,
                1,
                0
            ) as {{ var("symbol_name") }}_converted
        from {{ ref("user_deals") }}
        group by user_id, variant
    )

select
    user_id,
    variant,
    deals_cnt,
    percent_rank() over (
        partition by variant order by deals_cnt
    ) as deals_cnt_percentile,
    symbol_volume,
    percent_rank() over (
        partition by variant order by symbol_volume
    ) as symbol_volume_percentile,
    {{ var("symbol_name") }}_vol,
    percent_rank() over (
        partition by variant order by {{ var("symbol_name") }}_vol
    ) as {{ var("symbol_name") }}_vol_percentile,
    coalesce(
        cast(first_{{ var("symbol_name") }}_deal as string), 'N/A'
    ) as first_{{ var("symbol_name") }}_deal,
    {{ var("symbol_name") }}_deals_cnt,
    percent_rank() over (
        partition by variant order by {{ var("symbol_name") }}_deals_cnt
    ) as {{ var("symbol_name") }}_deals_cnt_percentile,
    {{ var("symbol_name") }}_converted
from stats
order by deals_cnt desc
