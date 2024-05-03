SELECT
  *
FROM
  (
    SELECT
      *,
      CASE
        WHEN event_number = 1 THEN timestamp
      END AS first_push
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              internal_customer_id,
              SAFE_CAST(user_id AS INT64) as user_id,
              timestamp,
              campaign_id,
              action_id,
              type,
              properties.campaign_name,
              properties.status,
              properties.error,
              properties.action_name,
              properties.action_type,
              properties.variant,
              properties.platform,
              ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY
                  timestamp ASC
              ) AS event_number
            FROM
              bloomreach_raw.campaign
            WHERE
              timestamp >= '2024-01-01'
          )
        WHERE
          SAFE_CAST(user_id AS INT64) IN (
            SELECT
              user_id
            FROM
              (
                WITH u AS (
                  SELECT
                    DISTINCT user_id
                  FROM
                    wh_raw.users
                  WHERE
                    EXTRACT(
                      YEAR
                      FROM
                        registered_dt
                    ) = 2024
                ),
                b AS (
                  SELECT
                    DISTINCT SAFE_CAST(user_id AS INT64) AS user_id
                  FROM
                    bloomreach_raw.campaign
                  WHERE
                    DATE(timestamp) BETWEEN '2024-01-01'
                    AND DATE(CURRENT_TIMESTAMP)
                    AND SAFE_CAST(user_id AS INT64) IN (
                      SELECT
                        user_id
                      FROM
                        u
                    )
                    AND properties.action_type = 'mobile notification'
                    AND properties.status = 'delivered'
                )
                SELECT
                  *
                FROM
                  b
              )
          )
          AND action_type = 'mobile notification'
          AND DATE(timestamp) BETWEEN '2024-01-01'
          AND DATE(CURRENT_TIMESTAMP)
          AND status IN ('delivered', 'failed')
      )
  ) AS `int_pushes`