WITH
eligible AS (
    SELECT
        properties.variant AS variant,
        SAFE_CAST(user_id AS INT64) AS user_id
    FROM {{ source("bloomreach", "campaign") }}
    WHERE
        campaign_id = "{{ var('campaign_id') }}"
        AND action_id = {{ var('action_id') }}
)

SELECT
    user_id,
    variant
FROM eligible
{% if var('random_users') == true %}
{% set random_users = [var('symbol_name'), "random"]|join('_') %}
<<<<<<< HEAD

UNION all
SELECT user_id, 'Variant B' as variant
=======
union all
select safe_cast(user_id as int) as user_id, 'Variant B' as variant
>>>>>>> 56b6d2edd9101f115033569e86bccd96c74048dc
FROM 
{{ ref(random_users) }}
{% endif %}
