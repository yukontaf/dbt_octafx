{% set event_types = dbt_utils.get_column_values(
    table=ref("feed_app_reading_base"), column="event_type"
) %}

-- {% do log(event_types, info=True) %}

