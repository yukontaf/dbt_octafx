WITH
    promo AS (
        SELECT
            ab.user_id
            , 'promotion' AS event
            , be.timestamp
        FROM {{ref('ab_users')}} AS ab
            INNER JOIN
                {{source('bloomreach', 'campaign')}} AS be
                ON ab.user_id = be.user_id
        WHERE campaign_id = '66223aafe3b419a0a4b87ae4'
            AND action_type = 'email'
            AND action_id = 48
            AND timestamp BETWEEN '2024-04-19' AND '2024-04-26'
            AND ab.variant = 'Variant A'
    )


    , payment_select AS (
        SELECT
            ab.user_id
            , 'payment_system_select' AS event
            , time AS timestamp
        FROM {{ref('ab_users')}} AS ab
            INNER JOIN {{ source('amplitude', 'events_octa_raw_deposit_payment_system_select') }} AS ps
                ON ab.user_id = ps.user_id
        WHERE time BETWEEN '2024-04-19' AND '2024-04-26'
            AND ab.variant = 'Variant A'
    )

SELECT
    user_id
    , event
    , timestamp
FROM promo
UNION ALL
SELECT
    user_id
    , event
    , timestamp
FROM payment_select
;
