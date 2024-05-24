

with source as (

    select * from {{ source('amplitude', 'events_octa_raw_web_feed_reading') }}

),

renamed as (

    select
        user_id,
        time,
        event_type,
        device_id,
        app,
        platform,
        os_name,
        os_version,
        browser,
        browser_version,
        ip,
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
        session_starting_channel

    from source

)

select * from renamed
where time >= '2024-05-01'