SELECT
    variant,
    COUNT(DISTINCT user_id) AS user_count,
    ROUND(AVG({{ var('symbol_name') }}_vol), 4)
        AS avg_vol_{{ var('symbol_name') }},
    ROUND(STDDEV({{ var('symbol_name') }}_vol), 4)
        AS std_volume_{{ var('symbol_name') }},
    SUM({{ var('symbol_name') }}_deals_cnt)
        AS total_deals_{{ var('symbol_name') }},
    SUM({{ var('symbol_name') }}_vol) AS vol_{{ var('symbol_name') }},
    SUM(symbol_volume) AS total_vol,
    SUM({{ var('symbol_name') }}_converted) AS total_converted,
    SUM(deals_cnt) AS total_deals
FROM
    {{ ref('ab_stats') }}
GROUP BY variant
