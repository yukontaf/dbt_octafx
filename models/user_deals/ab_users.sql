WITH
eligible AS (
    SELECT
        properties.variant AS variant,
        SAFE_CAST(user_id AS INT) AS user_id
    FROM {{ source("bloomreach", "campaign") }}
    WHERE
        campaign_id = '{{ var('campaign_id') }}'
        AND action_id = {{ var('action_id') }}
)

SELECT
    user_id,
    variant
FROM eligible
{% if var('random_users') == true %}

{% set random_users = [var('symbol_name'), "random"]|join('_') %}

UNION all
SELECT safe_cast(user_id as int), 'Variant B' as variant
FROM 

{{ ref(random_users) }}

{% endif %}
