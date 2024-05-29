-- get_columns.sql
{% set event_types = dbt_utils.get_column_values(
    table=ref("mobile_appsflyer"), column="event_name"
) %}

{% do log("Event Types: " ~ event_types, info=True) %}

-- Optionally also select them as a result in a query
select '{{ event_types }}' as event_types
