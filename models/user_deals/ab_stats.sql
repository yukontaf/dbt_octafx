SELECT
    ab.user_id,
    ab.variant,
    COUNT(DISTINCT d.operation_id) AS deals_cnt,
    SUM(d.volume) AS symbol_volume,
    SUM(
        CASE
            WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN d.volume ELSE 0
        END
    )
        AS {{ var('symbol_name') }}_vol,
    MIN(
        CASE
            WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN d.open_time_dt
        END
    )
        AS first_{{ var('symbol_name') }}_deal,
    SUM(CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN 1 ELSE 0 END)
        AS {{ var('symbol_name') }}_deals_cnt,
    IF(
        SUM(
            CASE WHEN d.symbol_name = '{{ var('symbol_name') }}' THEN 1 ELSE 0 END
        )
        > 0,
        1,
        0
    )
        AS {{ var('symbol_name') }}_converted
FROM
    {{ ref('ab_users') }} AS ab
LEFT JOIN {{ ref('user_deals') }} AS d
    ON
        ab.user_id = d.user_id
        AND DATE(
            d.close_time_dt
        ) BETWEEN '{{ var('start') }}' AND '{{ var('end') }}'
        AND d.variant IS NOT NULL
GROUP BY
    ab.user_id, ab.variant
