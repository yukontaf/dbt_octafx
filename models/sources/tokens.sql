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
        raw_properties.user_id,
        raw_properties.google_push_notification_id
    FROM
        source
)
SELECT
    *
FROM
    renamed
