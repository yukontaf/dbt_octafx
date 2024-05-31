-- get_columns.sql
{% set action_types = dbt_utils.get_column_values(
    table=ref("bloomreach_campaign"), column="action_type"
) %}

