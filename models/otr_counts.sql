-- Extract date boundaries for the current month to optimize repeated calculations
with
    current_month_boundaries as (
        select
            date_trunc(current_date(), month) as month_start,
            date_add(date_trunc(current_date(), month), interval 1 month) as month_end
    ),

    unique_active_users as (
        select distinct ur.user_id
        from {{source('wh_raw', 'trading_real_raw')}} ur
        where
            date(ur.open_time_dt) >= (select month_start from current_month_boundaries)
            and date(ur.open_time_dt) < (select month_end from current_month_boundaries)
            and ur.cmd < 2
    ),

    feed_app_reading_base as (
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
        from
            {{ source("amplitude", "events_octa_raw_app_feed_reading") }}
        where
            date(time) >= (select month_start from current_month_boundaries)
            and date(time) < (select month_end from current_month_boundaries)
    ),

    feed_web_reading_base as (
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
        from
            {{ source("amplitude", "events_octa_raw_web_feed_reading") }}
        where
            date(time) >= (select month_start from current_month_boundaries)
            and date(time) < (select month_end from current_month_boundaries)
    ),

    deals_source as (select * from {{ source("wh_raw", "trading_otr_deals_real") }}),
    accounts_source as (select * from {{ source("wh_raw", "trading_otr_accounts_real") }}),

    deals_renamed as (
        select
            id,
            order_id,
            position_id,
            account_id,
            symbol,
            direction,
            volume,
            price,
            profit,
            quote_timestamp,
            create_timestamp,
            shard
        from deals_source
    ),

    accounts_renamed as (
        select
            id,
            user_id,
            balance,
            bonus,
            margin,
            leverage,
            group_name,
            currency,
            readonly,
            deleted,
            created,
            shard
        from accounts_source
    ),

    current_month_deals as (
        select *
        from deals_renamed
        where
            date(create_timestamp) >= (select month_start from current_month_boundaries)
            and date(create_timestamp) < (select month_end from current_month_boundaries)
    ),

    user_deal_counts as (
        select account_id, count(*) as deal_count
        from current_month_deals
        group by account_id
    ),

    deal_user_mapping as (
        select udc.account_id, ar.user_id, coalesce(udc.deal_count, 0) as deal_count
        from user_deal_counts udc
        join accounts_renamed ar on udc.account_id = ar.id
    ),

    app_reading_counts as (
        select user_id, count(*) as app_reading_count
        from feed_app_reading_base
        group by user_id
    ),

    web_reading_counts as (
        select user_id, count(*) as web_reading_count
        from feed_web_reading_base
        group by user_id
    ),

    combined_reading_counts as (
        select
            coalesce(app.user_id, web.user_id) as user_id,
            coalesce(app.app_reading_count, 0) + coalesce(web.web_reading_count, 0) as total_reading_count
        from app_reading_counts app
        full outer join web_reading_counts web on app.user_id = web.user_id
    )

-- Final Query to Get User ID with Reading Counts and Deal Counts,
-- ensuring every user in unique_active_users is considered
select
    uau.user_id,
    coalesce(drm.account_id, null) as account_id,
    coalesce(drm.deal_count, 0) as deal_count,
    coalesce(crc.total_reading_count, 0) as total_reading_count
from unique_active_users uau
left join deal_user_mapping drm on uau.user_id = drm.user_id
left join combined_reading_counts crc on uau.user_id = crc.user_id
