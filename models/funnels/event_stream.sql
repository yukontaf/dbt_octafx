{{ dbt_product_analytics.event_stream(
    from=ref('int_pushes'),
    event_type_col="status",
    user_id_col="user_id",
    date_col="timestamp",
    start_date="2024-01-01",
    end_date="2024-02-01") }}


select count(distinct user_id) from {{ref('int_pushes')}}