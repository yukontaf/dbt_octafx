WITH
    promo AS (
        SELECT
            ab.user_id
            , "promotion" AS event
            , timestamp
        FROM {{ref('ab_users')}} AS ab
            INNER JOIN
               {{source('bloomreach', 'campaign')}}  AS be
                ON ab.user_id = be.user_id
                    AND ab.variant = "Variant A"
        WHERE campaign_id = "66223aafe3b419a0a4b87ae4"
            AND action_type = "email"
            AND timestamp BETWEEN "2024-04-19" AND "2024-04-26"
    )


    , payment_select AS (
        SELECT
            ab.user_id
            , "payment_system_select" AS event
            , time AS timestamp
        FROM {{ref('ab_users')}} AS ab
            INNER JOIN {{source('wh_raw', 'events_octa_raw_deposit_payment_system_select')}} AS ps
                ON ab.user_id = ps.user_id
        WHERE time BETWEEN "2024-04-19" AND "2024-04-26"
    )

SELECT
    user_id
    , "promo" AS event
    , timestamp
FROM promo
UNION ALL
SELECT
    user_id
    , "payment_select" AS event
    , timestamp
FROM payment_select
