{{ config(materialized='table') }}

SELECT
    c.country,
    c.code,
    ct.tier,
    concat("Tier ", ct.tier_id) as tier_name
FROM {{source('wh_raw', 'countries')}} AS c
LEFT JOIN
    {{source('wh_raw', 'countries_tiers')}} AS ct
    ON c.country = ct.country
