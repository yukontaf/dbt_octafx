-- get_columns.sql
{% set action_types = dbt_utils.get_column_values(
    table=ref("bloomreach_campaign"), column="action_type"
) %}

{% do log("Event Types: " ~ event_types, info=True) %}

-- Optionally also select them as a result in a query
select
    '{{ action_types }}' as action_types

    {% set status_types = dbt_utils.get_column_values(
        table=ref("bloomreach_campaign"), column="status"
    ) %}

select '{{ status_types }}' as status_types
