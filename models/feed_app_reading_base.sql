

with source as (

    select * from {{ source('amplitude', 'events_octa_raw_app_feed_reading') }}

),

renamed as (

    select
        event_id,
        user_id,
        device_id,
        event_type,
        time,
        app_version_f,
        app,
        app_version_ep,
        platform,
        os_version,
        device_brand,
        device_manufacturer,
        device_model,
        carrier,
        language,
        ip,
        adid,
        day_of_week,
        post_slug,
        value,
        channel_id,
        symbol,
        title,
        post_type,
        pattern_slugs,
        is_subscribed,
        num_of_subscriptions,
        subscriptions,
        session_starting_channel,
        visit_source,
        source_post_id

    from source

)

select * from renamed
where time >= '2024-05-01'

