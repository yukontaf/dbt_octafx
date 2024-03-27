WITH stats AS (
    SELECT 
        ab.user_id,
        ab.variant,
        COUNT(DISTINCT d.operation_id) AS deals_cnt,
        COALESCE(SUM(d.volume), 0) AS symbol_volume,
        COALESCE(SUM(CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN d.volume ELSE 0 END), 0) AS {{ var('symbol_name') }}_vol,
        MIN(CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN d.open_time_dt END) AS first_{{ var('symbol_name') }}_deal,
        COALESCE(SUM(CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN 1 ELSE 0 END), 0) AS {{ var('symbol_name') }}_deals_cnt,
        IF(COALESCE(SUM(CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN 1 ELSE 0 END), 0) > 0, 1, 0) AS {{ var('symbol_name') }}_converted
    FROM 
        {{ ref('ab_users') }} AS ab
        LEFT JOIN {{ ref('user_deals') }} AS d ON ab.user_id = d.user_id
            AND DATE(d.close_time_dt) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}'
            AND d.variant IS NOT NULL
    GROUP BY 
        ab.user_id, ab.variant
)
SELECT 
    user_id,
    variant,
    deals_cnt,
    PERCENT_RANK() OVER (PARTITION BY variant ORDER BY deals_cnt) AS deals_cnt_percentile,
    symbol_volume,
    PERCENT_RANK() OVER (PARTITION BY variant ORDER BY symbol_volume) AS symbol_volume_percentile,
    {{ var('symbol_name') }}_vol,
    PERCENT_RANK() OVER (PARTITION BY variant ORDER BY {{ var('symbol_name') }}_vol) AS {{ var('symbol_name') }}_vol_percentile,
    COALESCE(CAST(first_{{ var('symbol_name') }}_deal AS STRING), 'N/A') AS first_{{ var('symbol_name') }}_deal,
    {{ var('symbol_name') }}_deals_cnt,
    PERCENT_RANK() OVER (PARTITION BY variant ORDER BY {{ var('symbol_name') }}_deals_cnt) AS {{ var('symbol_name') }}_deals_cnt_percentile,
    {{ var('symbol_name') }}_converted
FROM 
    stats
ORDER BY 
    deals_cnt DESC