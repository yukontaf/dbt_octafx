{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(tags=["base_macro"]) }}

-- page view context is given as json string in csv. Parse json
with
    prep as (
        select
            * except (
                contexts_com_snowplowanalytics_user_identifier_1_0_0,
                contexts_com_snowplowanalytics_user_identifier_2_0_0,
                contexts_com_snowplowanalytics_session_identifier_1_0_0,
                contexts_com_snowplowanalytics_session_identifier_2_0_0,
                contexts_com_snowplowanalytics_custom_entity_1_0_0
            ),
            from_json(
                contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
                'array<struct<id:string>>'
            ) as contexts_com_snowplowanalytics_snowplow_web_page_1,
            from_json(
                unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
                'array<struct<basis_for_processing:string, consent_scopes:array<string>, consent_url:string, consent_version:string, domains_applied:array<string>, event_type:string, gdpr_applies:string>>'
            ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
            from_json(
                unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,
                'array<struct<elapsed_time:string>>'
            ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
            from_json(
                contexts_com_iab_snowplow_spiders_and_robots_1_0_0,
                'array<struct<category:string,primaryImpact:string,reason:string,spiderOrRobot:boolean>>'
            ) as contexts_com_iab_snowplow_spiders_and_robots_1,
            from_json(
                contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,
                'array<struct<deviceFamily:string,osFamily:string,osMajor:string,osMinor:string,osPatch:string,osPatchMinor:string,osVersion:string,useragentFamily:string,useragentMajor:string,useragentMinor:string,useragentPatch:string,useragentVersion:string>>'
            ) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
            from_json(
                contexts_nl_basjes_yauaa_context_1_0_0,
                'array<struct<agentClass:string,agentInformationEmail:string,agentName:string,agentNameVersion:string,agentNameVersionMajor:string,agentVersion:string,agentVersionMajor:string,deviceBrand:string,deviceClass:string,deviceCpu:string,deviceCpuBits:string,deviceName:string,deviceVersion:string,layoutEngineClass:string,layoutEngineName:string,layoutEngineNameVersion:string,layoutEngineNameVersionMajor:string,layoutEngineVersion:string,layoutEngineVersionMajor:string,networkType:string,operatingSystemClass:string,operatingSystemName:string,operatingSystemNameVersion:string,operatingSystemNameVersionMajor:string,operatingSystemVersion:string,operatingSystemVersionBuild:string,operatingSystemVersionMajor:string,webviewAppName:string,webviewAppNameVersionMajor:string,webviewAppVersion:string,webviewAppVersionMajor:string>>'
            ) as contexts_nl_basjes_yauaa_context_1,
            from_json(
                contexts_com_snowplowanalytics_user_identifier_1_0_0,
                'array<struct<user_id:string>>'
            ) as contexts_com_snowplowanalytics_user_identifier_1,
            from_json(
                contexts_com_snowplowanalytics_user_identifier_2_0_0,
                'array<struct<user_id:string>>'
            ) as contexts_com_snowplowanalytics_user_identifier_2,
            from_json(
                contexts_com_snowplowanalytics_session_identifier_1_0_0,
                'array<struct<session_id:string>>'
            ) as contexts_com_snowplowanalytics_session_identifier_1,
            from_json(
                contexts_com_snowplowanalytics_session_identifier_2_0_0,
                'array<struct<session_identifier:string>>'
            ) as contexts_com_snowplowanalytics_session_identifier_2,
            from_json(
                contexts_com_snowplowanalytics_custom_entity_1_0_0,
                'array<struct<contents:string>>'
            ) as contexts_com_snowplowanalytics_custom_entity_1
        from {{ ref("snowplow_events") }}
    )

select
    app_id,
    platform,
    etl_tstamp,
    collector_tstamp,
    dvce_created_tstamp,
    event,
    event_id,
    txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    se_category,
    se_action,
    se_label,
    se_property,
    se_value,
    tr_orderid,
    tr_affiliation,
    tr_total,
    tr_tax,
    tr_shipping,
    tr_city,
    tr_state,
    tr_country,
    ti_orderid,
    ti_sku,
    ti_name,
    ti_category,
    ti_price,
    ti_quantity,
    pp_xoffset_min,
    pp_xoffset_max,
    pp_yoffset_min,
    pp_yoffset_max,
    useragent,
    br_name,
    br_family,
    br_version,
    br_type,
    br_renderengine,
    br_lang,
    br_features_pdf,
    br_features_flash,
    br_features_java,
    br_features_director,
    br_features_quicktime,
    br_features_realplayer,
    br_features_windowsmedia,
    br_features_gears,
    br_features_silverlight,
    br_cookies,
    br_colordepth,
    br_viewwidth,
    br_viewheight,
    os_name,
    os_family,
    os_manufacturer,
    os_timezone,
    dvce_type,
    dvce_ismobile,
    dvce_screenwidth,
    dvce_screenheight,
    doc_charset,
    doc_width,
    doc_height,
    tr_currency,
    tr_total_base,
    tr_tax_base,
    tr_shipping_base,
    ti_currency,
    ti_price_base,
    base_currency,
    geo_timezone,
    mkt_clickid,
    mkt_network,
    etl_tags,
    dvce_sent_tstamp,
    refr_domain_userid,
    refr_dvce_tstamp,
    domain_sessionid,
    derived_tstamp,
    event_vendor,
    event_name,
    event_format,
    event_version,
    event_fingerprint,
    true_tstamp,
    load_tstamp,
    contexts_com_snowplowanalytics_snowplow_web_page_1,
    struct(
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].basis_for_processing::string as basis_for_processing,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].consent_scopes::array<string> as consent_scopes,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].consent_url::string as consent_url,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].consent_version::string as consent_version,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].domains_applied::array<string> as domains_applied,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].event_type::string as event_type,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[
            0
        ].gdpr_applies::boolean as gdpr_applies
    ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
    struct(
        unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1[
            0
        ].elapsed_time::float as elapsed_time
    ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
    array(
        struct(
            contexts_com_iab_snowplow_spiders_and_robots_1[0].category as category,
            contexts_com_iab_snowplow_spiders_and_robots_1[
                0
            ].primaryimpact as primary_impact,
            contexts_com_iab_snowplow_spiders_and_robots_1[0].reason as reason,
            contexts_com_iab_snowplow_spiders_and_robots_1[
                0
            ].spiderorrobot as spider_or_robot
        )
    ) as contexts_com_iab_snowplow_spiders_and_robots_1,
    array(
        struct(
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].devicefamily as device_family,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].osfamily as os_family,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].osmajor as os_major,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].osminor as os_minor,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].ospatch as os_patch,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].ospatchminor as os_patch_minor,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].osversion as os_version,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].useragentfamily as useragent_family,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].useragentmajor as useragent_major,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].useragentminor as useragent_minor,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].useragentpatch as useragent_patch,
            contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[
                0
            ].useragentversion as useragent_version
        )
    ) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
    array(
        struct(
            contexts_nl_basjes_yauaa_context_1[0].agentclass as agent_class,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].agentinformationemail as agent_information_email,
            contexts_nl_basjes_yauaa_context_1[0].agentname as agent_name,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].agentnameversion as agent_name_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].agentnameversionmajor as agent_name_version_major,
            contexts_nl_basjes_yauaa_context_1[0].agentversion as agent_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].agentversionmajor as agent_version_major,
            contexts_nl_basjes_yauaa_context_1[0].devicebrand as device_brand,
            contexts_nl_basjes_yauaa_context_1[0].deviceclass as device_class,
            contexts_nl_basjes_yauaa_context_1[0].devicecpu as device_cpu,
            contexts_nl_basjes_yauaa_context_1[0].devicecpubits as device_cpu_bits,
            contexts_nl_basjes_yauaa_context_1[0].devicename as device_name,
            contexts_nl_basjes_yauaa_context_1[0].deviceversion as device_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutengineclass as layout_engine_class,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutenginename as layout_engine_name,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutenginenameversion as layout_engine_name_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutenginenameversionmajor as layout_engine_name_version_major,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutengineversion as layout_engine_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].layoutengineversionmajor as layout_engine_version_major,
            contexts_nl_basjes_yauaa_context_1[0].networktype as network_type,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemclass as operating_system_class,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemname as operating_system_name,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemnameversion as operating_system_name_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemnameversionmajor as operating_system_name_version_major,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemversion as operating_system_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemversionbuild as operating_system_version_build,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].operatingsystemversionmajor as operating_system_version_major,
            contexts_nl_basjes_yauaa_context_1[0].webviewappname as webview_app_name,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].webviewappnameversionmajor as webview_app_name_version_major,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].webviewappversion as webview_app_version,
            contexts_nl_basjes_yauaa_context_1[
                0
            ].webviewappversionmajor as webview_app_version_major
        )
    ) as contexts_nl_basjes_yauaa_context_1,
    array(
        struct(contexts_com_snowplowanalytics_user_identifier_1[0].user_id as user_id)
    ) as contexts_com_snowplowanalytics_user_identifier_1,
    array(
        struct(contexts_com_snowplowanalytics_user_identifier_2[0].user_id as user_id)
    ) as contexts_com_snowplowanalytics_user_identifier_2,
    array(
        struct(
            contexts_com_snowplowanalytics_session_identifier_1[
                0
            ].session_id as session_id
        )
    ) as contexts_com_snowplowanalytics_session_identifier_1,
    array(
        struct(
            contexts_com_snowplowanalytics_session_identifier_2[
                0
            ].session_identifier as session_identifier
        )
    ) as contexts_com_snowplowanalytics_session_identifier_2,
    {% if var("snowplow__custom_test", false) %}
        array(
            struct(
                contexts_com_snowplowanalytics_custom_entity_1[0].contents as contents
            )
        ) as contexts_com_snowplowanalytics_custom_entity_1
    {% else %} null as contexts_com_snowplowanalytics_custom_entity_1
    {% endif %}
from prep
