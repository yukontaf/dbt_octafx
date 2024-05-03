SELECT
    af.id,
    af.appsflyer_id,
    -- , af_id.user_id
    media_source,
    af_channel,
    campaign,
    country_code,
    af.ip,
    af.platform,
    af.device_type,
    af.os_version,
    af.app_version,
    af.sdk_version,
    af.app_id,
    af.app_name,
    af.bundle_id,
    af.event_name,
    af.event_value,
    af.device_model,
    af.device_brand,
    af.event_type,
    af.event_time_dt,
    af.install_time_dt,
    af.store_reinstall,
    af.install_app_store
FROM `analytics-147612`.`wh_raw`.`mobile_appsflyer` AS af
WHERE af.event_time_dt >= '2024-01-01'