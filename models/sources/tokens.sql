WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'bloomreach',
            'customers_properties'
        ) }}
),
renamed AS (
    SELECT
        properties.user_id,
        properties.google_push_notification_id
    FROM
        source
)
SELECT
    *
FROM
    renamed
