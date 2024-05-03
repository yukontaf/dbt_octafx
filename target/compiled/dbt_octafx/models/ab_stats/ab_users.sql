WITH
eligible AS (
    SELECT
        properties.variant,
        SAFE_CAST(user_id AS INT64) AS user_id
    FROM `analytics-147612`.`bloomreach_raw`.`campaign`
    WHERE
        campaign_id = "65733b51df03a1daadeb772d"
        AND action_id = 437
)

SELECT
    user_id,
    varianto
FROM eligible
