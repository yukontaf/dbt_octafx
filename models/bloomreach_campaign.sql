{{ config(
    materialized='view'
) }}

select distinct
    internal_customer_id,
    type,
    ingest_timestamp,
    timestamp,
    properties.__platform__a8ad3f04,
    properties.__versioncode__f4faa1c0,
    properties._platform,
    properties.action_name,
    properties.action_type,
    properties.action_url,
    properties.app_version,
    properties.attempts,
    properties.body,
    properties.body2,
    properties.bot,
    properties.browser,
    properties.c_language_code,
    properties.c_subject,
    properties.c_subject_rendered,
    properties.c_webinar_event_slug,
    properties.c_webinar_tag,
    properties.campaign_name,
    properties.campaign_policy,
    properties.campaign_trigger,
    properties.channel_id,
    properties.city,
    properties.code,
    properties.comment,
    properties.consent_category,
    properties.country,
    properties.delta_time,
    properties.device,
    properties.error,
    properties.event_id,
    properties.event_type,
    properties.google_push_notification_id,
    properties.id,
    properties.integration_id,
    properties.integration_name,
    properties.iosversionsode__d77c073d,
    properties.ip,
    properties.is_web,
    properties.language,
    properties.latitude,
    properties.location,
    properties.longitude,
    properties.message,
    properties.message_id,
    properties.message_type,
    properties.new_choice,
    properties.os,
    properties.platform,
    properties.platformslug__10ba264c,
    properties.positionid__7c012ec8,
    properties.post_slug,
    properties.postid__62fcd071,
    properties.publicationurl__233f7608,
    properties.recipient,
    properties.redirect_to_screen,
    properties.sender,
    properties.sending_ip,
    properties.sent_timestamp,
    properties.slug,
    properties.state,
    properties.status,
    properties.status_code,
    properties.storyly_group_id,
    properties.subject,
    properties.symbol,
    properties.template_id,
    properties.template_name,
    properties.title,
    properties.url,
    properties.user_agent__a510e506,
    properties.utm_campaign,
    properties.utm_content,
    properties.utm_medium,
    properties.utm_source,
    properties.valid_until,
    properties.variant,
    properties.variant_id,
    properties.xpath,
    safe_cast(user_id as int64) as user_id,
    campaign_id,
    action_id
from {{source("bloomreach", "campaign")}}
where extract(year from timestamp) = extract(year from current_date())
and user_id in (select user_id from {{ref('users_segment')}})
