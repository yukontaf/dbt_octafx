-- file: models/tw_segment_space_event_counts.sql
with
    actual_tw_space_users as (
        select distinct user_id from {{ ref("trading_real_raw") }}
    ),

    app_feed_events as (
        select
            user_id,
            countif(
                extract(month from time) = extract(month from current_date)
                and extract(year from time) = extract(year from current_date)
            ) as app_event_count_month,
            countif(
                extract(year from time) = extract(year from current_date)
            ) as app_event_count_year
        from {{ ref("feed_app_reading_base") }}
        group by user_id
    ),

    web_feed_events as (
        select
            user_id,
            countif(
                extract(month from time) = extract(month from current_date)
                and extract(year from time) = extract(year from current_date)
            ) as web_event_count_month,
            countif(
                extract(year from time) = extract(year from current_date)
            ) as web_event_count_year
        from {{ ref("feed_web_reading_base") }}
        group by user_id
    ),

    total_events as (
        select
            u.user_id,
            coalesce(a.app_event_count_month, 0) as app_event_count_month,
            coalesce(a.app_event_count_year, 0) as app_event_count_year,
            coalesce(w.web_event_count_month, 0) as web_event_count_month,
            coalesce(w.web_event_count_year, 0) as web_event_count_year,
            coalesce(a.app_event_count_month, 0)
            + coalesce(w.web_event_count_month, 0) as total_event_count_month,
            coalesce(a.app_event_count_year, 0)
            + coalesce(w.web_event_count_year, 0) as total_event_count_year
        from actual_tw_space_users u
        left join app_feed_events a on u.user_id = a.user_id
        left join web_feed_events w on u.user_id = w.user_id
    )

select *
from total_events
