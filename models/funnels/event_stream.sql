{{ config(
        materialized='view',
        partition_by={
            "field": "timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        clustering=["user_id", "event_nature"]
    ) }}

{% set start_date = '2024-04-01' %}
{% set end_date = '2024-04-10' %}

WITH bloomreach_events AS (
    SELECT
        action_type AS event_type,
        timestamp,
        cast(user_id as int) user_id,
        'communication' AS event_nature
    FROM
        {{ref('funnel_bloomreach_events')}}
    WHERE
        timestamp >= '{{ start_date }}' AND
        timestamp <= '{{ end_date }}'
),

deals AS (
    SELECT
        cast(cmd as string) AS event_type,
        timestamp,
        user_id,
        'deal' AS event_nature
    FROM
        {{ref('funnel_deals')}}
    WHERE
        timestamp >= '{{ start_date }}' AND
        timestamp <= '{{ end_date }}'
)

SELECT * FROM bloomreach_events
UNION ALL
SELECT * FROM deals
ORDER BY user_id, timestamp