select
    c.type,
    c.ingest_timestamp,
    c.timestamp as event_tstmp,
    properties.action_type,
    properties.campaign_trigger,
    properties.language,
    properties.platform,
    properties.campaign_name,
    properties.action_id,
    properties.campaign_policy,
    -- properties.subject,
    properties.action_name,
    properties.recipient,
    -- properties.message,
    properties.sent_timestamp,
    properties.consent_category,
    properties.campaign_id,
    properties.status,
    properties.integration_id,
    properties.message_id,
    properties.integration_name,
    properties.utm_campaign,
    properties.utm_medium,
    properties.utm_source,
    properties.code,
    properties.message_type,
    properties.sender,
    properties.sending_ip,
    properties.country,
    properties.city,
    properties.ip,
    properties.state,
    properties.longitude,
    properties.status_code,
    properties.error,
    properties.event_type,
    properties.symbol,
    properties.redirect_to_screen,
    properties.template_name,
    properties.title,
    properties.attempts,
    properties.delta_time,
    properties.os,
    properties.location,
    properties.device,
    properties.action_url,
    safe_cast(c.user_id as int) as user_id
from {{ source("bloomreach", 'campaign') }} as c
where
    timestamp_trunc(c.timestamp, day) >= '2024-01-01'
