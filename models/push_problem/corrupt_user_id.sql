SELECT
  `failed_pushes_users`.appsflyer_id `failed_pushes_users__appsflyer_id`,
  `failed_pushes_users`.user_id `failed_pushes_users__user_id`
FROM
  (
    SELECT
      e.user_id,
      a.appsflyer_id
    FROM
      `analytics-147612`.`dev_gsokolov`.`int_bloomreach_events_enhanced` e
      INNER JOIN `analytics-147612`.`dev_gsokolov`.`int_af_id` a ON e.user_id = a.user_id
    WHERE
      e.timestamp > '2024-01-01T00:00:00.000'
      AND e.timestamp < '2024-12-31T23:59:59.999'
      AND (
        e.action_type = 'mobile notification'
      )
      AND (e.status = 'failed')
      AND (a.appsflyer_id is not null)
    GROUP BY
      1,
      2
  ) AS `failed_pushes_users`
SELECT
  `failed_pushes_users`.appsflyer_id `failed_pushes_users__appsflyer_id`,
  `failed_pushes_users`.user_id `failed_pushes_users__user_id`
FROM
  (
    SELECT
      e.user_id,
      a.appsflyer_id
    FROM
      `analytics-147612`.`dev_gsokolov`.`int_bloomreach_events_enhanced` e
      INNER JOIN `analytics-147612`.`dev_gsokolov`.`int_af_id` a ON e.user_id = a.user_id
    WHERE
      e.timestamp > '2024-01-01T00:00:00.000'
      AND e.timestamp < '2024-12-31T23:59:59.999'
      AND (
        e.action_type = 'mobile notification'
      )
      AND (e.status = 'failed')
      AND (a.appsflyer_id is not null)
    GROUP BY
      1,
      2
  ) AS `failed_pushes_users`
GROUP BY
  1,
  2