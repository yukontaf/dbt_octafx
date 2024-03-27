WITH
eligible AS (
    SELECT
        properties.variant AS variant,
        SAFE_CAST(user_id AS INT64) AS user_id
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
union all
select user_id, 'Variant B' as variant
from 
{{ source('supplement', 'random_users') }}
{% endif %}
