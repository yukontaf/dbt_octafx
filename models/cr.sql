{{ config(materialized="view") }}

{% set start_date = "2024-01-01" %}
{% set end_date = "2024-01-08" %}
{% set conversion_window_days = 30 %}
{% set filter_campaign_ids = ["123", "456", "789"] %}

with
    -- Step 1: Filter bloomreach_campaign data
    campaign_data as (
        select distinct
            internal_customer_id,
            ingest_timestamp,
            safe_cast(user_id as int64) as user_id,
            campaign_id,
            action_id,
            timestamp as campaign_timestamp
        from {{ ref("bloomreach_campaign") }}
        where
            timestamp
            between timestamp('{{ start_date }}') and timestamp('{{ end_date }}')
            and extract(year from timestamp) = extract(year from current_date())
            and user_id
            in (select safe_cast(user_id as int64) from {{ ref("users_segment") }})
            and campaign_id in ({{ filter_campaign_ids | join("', '") }})
    ),

    -- Step 2: Filter and process relevant deposits_enhanced data
    deposits_data as (
        with
            source as (select * from {{ source("wh_raw", "deposits_enhanced") }}),

            renamed as (
                select user_id, deposit_id, amount, created_dt
                from source
                where extract(year from created_dt) = extract(year from current_date())
            )
        select *
        from renamed
    ),

    -- Step 3: Join campaign_data with deposits_data to identify conversions
    campaign_deposit_conversion as (
        select
            camp.internal_customer_id,
            camp.user_id,
            camp.campaign_id,
            camp.action_id,
            camp.campaign_timestamp,
            depo.created_dt as deposit_time,
            depo.deposit_id,
            depo.amount as deposit_amount,
            if(depo.created_dt is not null, 1, 0) as conversion_flag
        from campaign_data camp
        left join
            deposits_data depo
            on camp.user_id = depo.user_id
            and depo.created_dt between camp.campaign_timestamp and timestamp_add(
                camp.campaign_timestamp, interval {{ conversion_window_days }} day
            )
    )

-- Step 4: Aggregate the conversion data
select
    user_id,
    campaign_id,
    action_id,
    count(case when conversion_flag = 1 then 1 else null end) as conversions,
    count(*) as total_campaigns,
    (
        count(case when conversion_flag = 1 then 1 else null end) * 1.0 / count(*)
    ) as conversion_rate
from campaign_deposit_conversion
group by user_id, campaign_id, action_id
