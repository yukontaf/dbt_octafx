with source as (

    select * from {{ source('wh_raw', 'appsflyer_uninstall_events_report') }}

),

renamed as (

    select
        install_time,
        event_time,
        event_value,
        campaign,
        campaign_id,
        region,
        country_code,
        appsflyer_id,
        customer_user_id,
        platform,
        os_version,
        app_version,
        sdk_version,
        app_id,
        app_name,
        bundle_id,
        user_agent,
        google_play_referrer,
        google_play_click_time,
        google_play_install_begin_time

    from source

)

select * from renamed
