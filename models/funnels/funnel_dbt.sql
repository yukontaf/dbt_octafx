{% set events =
  dbt_product_analytics.event_stream(
    from=ref('event_stream'),
    event_type_col="event_nature",
    user_id_col="user_id",
    date_col="timestamp"
    )
%}

{% set steps = ["communication", "deal"] %}

{{ dbt_product_analytics.funnel(steps=steps, event_stream=events) }}
