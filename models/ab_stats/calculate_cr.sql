{% set events =
  dbt_product_analytics.event_stream(
    from=ref('promo_event_stream'),
    event_type_col="status",
    user_id_col="user_id",
    date_col="timestamp",
    start_date="2034-04-19",
    end_date="2024-04-26")
%}

{% set steps = ["promo", "completed" ] %}

{{ dbt_product_analytics.funnel(steps=steps, event_stream=events) }}
-- or materialize your event stream and use:
-- {{ dbt_product_analytics.funnel(steps=steps, event_stream=ref('order_events')) }}
