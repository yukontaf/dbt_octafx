SELECT
    e.*,
{{ dbt_utils.star(from=ref('int_af_id'), except=['user_id'], relation_alias='i') }},
  i.user_id AS af_user_id
FROM
    {{ ref('int_bloomreach_events_enhanced') }} AS e
LEFT JOIN {{ ref('int_af_id') }} AS i ON e.user_id = i.user_id
WHERE
    e.action_type = 'mobile notification'
