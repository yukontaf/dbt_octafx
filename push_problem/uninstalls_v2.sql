WITH
          u AS (
              SELECT DISTINCT user_id
              FROM wh_raw.users
              WHERE EXTRACT(YEAR FROM registered_dt) = 2024
          ),

          b AS (
              SELECT DISTINCT SAFE_CAST(user_id AS INT64) AS user_id
              FROM bloomreach_raw.campaign WHERE
                  DATE(timestamp) BETWEEN '2024-01-01' AND DATE(CURRENT_TIMESTAMP)
                  AND SAFE_CAST(user_id AS INT64) IN (SELECT user_id FROM u)
                  AND properties.action_type = 'mobile notification'
                  AND properties.status = 'delivered'
          ),

          int_pushes AS (
              SELECT
                  timestamp,
                  'delivered' AS status,
                  SAFE_CAST(user_id AS INT64) AS user_id
              FROM bloomreach_raw.campaign
              WHERE
                  properties.action_type = 'mobile notification'
                  AND properties.status = 'failed'
          ),

          push_status AS (
              SELECT
                  p.user_id,
                  p.timestamp AS push_timestamp,
                  u.uninstalled_at,
                  CASE
                      WHEN p.timestamp < u.uninstalled_at THEN 0
                      ELSE 1
                  END AS status
              FROM int_pushes AS p
                  INNER JOIN dev_gsokolov.uninstalls AS u ON p.user_id = u.user_id
          )

          SELECT
              ps.user_id,
              ps.push_timestamp,
              ps.uninstalled_at,
              ps.status
          FROM push_status AS ps`,
