{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(tags=["base_macro"]) }}

-- page view context is given as json string in csv. Extract array from json
with
    prep as (
        select
            * except (
                contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
                unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
                unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,
                contexts_com_iab_snowplow_spiders_and_robots_1_0_0,
                contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,
                contexts_nl_basjes_yauaa_context_1_0_0,
                contexts_com_snowplowanalytics_user_identifier_1_0_0,
                contexts_com_snowplowanalytics_user_identifier_2_0_0,
                contexts_com_snowplowanalytics_session_identifier_1_0_0,
                contexts_com_snowplowanalytics_session_identifier_2_0_0,
                contexts_com_snowplowanalytics_custom_entity_1_0_0
            ),
            json_extract_array(
                contexts_com_snowplowanalytics_snowplow_web_page_1_0_0
            ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
            json_extract_array(
                unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0
            )
            as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
            json_extract_array(
                unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0
            ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,
            json_extract_array(
                contexts_com_iab_snowplow_spiders_and_robots_1_0_0
            ) as contexts_com_iab_snowplow_spiders_and_robots_1_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0
            ) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,
            json_extract_array(
                contexts_nl_basjes_yauaa_context_1_0_0
            ) as contexts_nl_basjes_yauaa_context_1_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_user_identifier_1_0_0
            ) as contexts_com_snowplowanalytics_user_identifier_1_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_user_identifier_2_0_0
            ) as contexts_com_snowplowanalytics_user_identifier_2_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_session_identifier_1_0_0
            ) as contexts_com_snowplowanalytics_session_identifier_1_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_session_identifier_2_0_0
            ) as contexts_com_snowplowanalytics_session_identifier_2_0_0,
            json_extract_array(
                contexts_com_snowplowanalytics_custom_entity_1_0_0
            ) as contexts_com_snowplowanalytics_custom_entity_1_0_0,
        from {{ ref("snowplow_events") }}
    )

-- recreate repeated record field i.e. array of structs as is originally in BQ events
-- table
select
    * except (
        contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
        unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
        unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,
        contexts_com_iab_snowplow_spiders_and_robots_1_0_0,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,
        contexts_nl_basjes_yauaa_context_1_0_0,
        contexts_com_snowplowanalytics_user_identifier_1_0_0,
        contexts_com_snowplowanalytics_user_identifier_2_0_0,
        contexts_com_snowplowanalytics_session_identifier_1_0_0,
        contexts_com_snowplowanalytics_session_identifier_2_0_0,
        contexts_com_snowplowanalytics_custom_entity_1_0_0
    ),
    array(
        select as struct json_extract_scalar(json_array, '$.id') as id
        from
            unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,

    array(
        select as struct
            json_extract_scalar(
                json_array, '$.basis_for_processing'
            ) as basis_for_processing,
            json_extract_string_array(json_array, '$.consent_scopes') as consent_scopes,
            json_extract_scalar(json_array, '$.consent_url') as consent_url,
            json_extract_scalar(json_array, '$.consent_version') as consent_version,
            json_extract_string_array(
                json_array, '$.domains_applied'
            ) as domains_applied,
            json_extract_scalar(json_array, '$.event_type') as event_type,
            json_extract_scalar(json_array, '$.gdpr_applies') as gdpr_applies
        from
            unnest(
                unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0
            ) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,

    array(
        select as struct
            json_extract_scalar(json_array, '$.elapsed_time') as elapsed_time
        from
            unnest(
                unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0
            ) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,

    array(
        select as struct
            json_extract_scalar(json_array, '$.category') as category,
            json_extract_scalar(json_array, '$.primaryImpact') as primary_impact,
            json_extract_scalar(json_array, '$.reason') as reason,
            cast(
                json_extract_scalar(json_array, '$.spiderOrRobot') as boolean
            ) as spider_or_robot
        from unnest(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as json_array
    ) as contexts_com_iab_snowplow_spiders_and_robots_1_0_0,

    array(
        select as struct
            json_extract_scalar(json_array, '$.deviceFamily') as device_family,
            json_extract_scalar(json_array, '$.osFamily') as os_family,
            json_extract_scalar(json_array, '$.osMajor') as os_major,
            json_extract_scalar(json_array, '$.osMinor') as os_minor,
            json_extract_scalar(json_array, '$.osPatch') as os_patch,
            json_extract_scalar(json_array, '$.osPatchMinor') as os_patch_minor,
            json_extract_scalar(json_array, '$.osVersion') as os_version,
            json_extract_scalar(json_array, '$.useragentFamily') as useragent_family,
            json_extract_scalar(json_array, '$.useragentMajor') as useragent_major,
            json_extract_scalar(json_array, '$.useragentMinor') as useragent_minor,
            json_extract_scalar(json_array, '$.useragentPatch') as useragent_patch,
            json_extract_scalar(json_array, '$.useragentVersion') as useragent_version
        from
            unnest(
                contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0
            ) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,

    array(
        select as struct
            json_extract_scalar(json_array, '$.agentClass') as agent_class,
            json_extract_scalar(
                json_array, '$.agentInformationEmail'
            ) as agent_information_email,
            json_extract_scalar(json_array, '$.agentName') as agent_name,
            json_extract_scalar(json_array, '$.agentNameVersion') as agent_name_version,
            json_extract_scalar(
                json_array, '$.agentNameVersionMajor'
            ) as agent_name_version_major,
            json_extract_scalar(json_array, '$.agentVersion') as agent_version,
            json_extract_scalar(
                json_array, '$.agentVersionMajor'
            ) as agent_version_major,
            json_extract_scalar(json_array, '$.deviceBrand') as device_brand,
            json_extract_scalar(json_array, '$.deviceClass') as device_class,
            json_extract_scalar(json_array, '$.deviceCpu') as device_cpu,
            json_extract_scalar(json_array, '$.deviceCpuBits') as device_cpu_bits,
            json_extract_scalar(json_array, '$.deviceName') as device_name,
            json_extract_scalar(json_array, '$.deviceVersion') as device_version,
            json_extract_scalar(
                json_array, '$.layoutEngineClass'
            ) as layout_engine_class,
            json_extract_scalar(json_array, '$.layoutEngineName') as layout_engine_name,
            json_extract_scalar(
                json_array, '$.layoutEngineNameVersion'
            ) as layout_engine_name_version,
            json_extract_scalar(
                json_array, '$.layoutEngineNameVersionMajor'
            ) as layout_engine_name_version_major,
            json_extract_scalar(
                json_array, '$.layoutEngineVersion'
            ) as layout_engine_version,
            json_extract_scalar(
                json_array, '$.layoutEngineVersionMajor'
            ) as layout_engine_version_major,
            json_extract_scalar(json_array, '$.networkType') as network_type,
            json_extract_scalar(
                json_array, '$.operatingSystemClass'
            ) as operating_system_class,
            json_extract_scalar(
                json_array, '$.operatingSystemName'
            ) as operating_system_name,
            json_extract_scalar(
                json_array, '$.operatingSystemNameVersion'
            ) as operating_system_name_version,
            json_extract_scalar(
                json_array, '$.operatingSystemNameVersionMajor'
            ) as operating_system_name_version_major,
            json_extract_scalar(
                json_array, '$.operatingSystemVersion'
            ) as operating_system_version,
            json_extract_scalar(
                json_array, '$.operatingSystemVersionBuild'
            ) as operating_system_version_build,
            json_extract_scalar(
                json_array, '$.operatingSystemVersionMajor'
            ) as operating_system_version_major,
            json_extract_scalar(json_array, '$.webviewAppName') as webview_app_name,
            json_extract_scalar(
                json_array, '$.webviewAppNameVersionMajor'
            ) as webview_app_name_version_major,
            json_extract_scalar(
                json_array, '$.webviewAppVersion'
            ) as webview_app_version,
            json_extract_scalar(
                json_array, '$.webviewAppVersionMajor'
            ) as webview_app_version_major
        from unnest(contexts_nl_basjes_yauaa_context_1_0_0) as json_array
    ) as contexts_nl_basjes_yauaa_context_1_0_0,
    array(
        select as struct json_extract_scalar(json_array, '$.user_id') as user_id
        from unnest(contexts_com_snowplowanalytics_user_identifier_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_user_identifier_1_0_0,
    array(
        select as struct json_extract_scalar(json_array, '$.user_id') as user_id
        from unnest(contexts_com_snowplowanalytics_user_identifier_2_0_0) as json_array
    ) as contexts_com_snowplowanalytics_user_identifier_2_0_0,
    array(
        select as struct json_extract_scalar(json_array, '$.session_id') as session_id
        from
            unnest(
                contexts_com_snowplowanalytics_session_identifier_1_0_0
            ) as json_array
    ) as contexts_com_snowplowanalytics_session_identifier_1_0_0,
    array(
        select as struct
            json_extract_scalar(
                json_array, '$.session_identifier'
            ) as session_identifier
        from
            unnest(
                contexts_com_snowplowanalytics_session_identifier_2_0_0
            ) as json_array
    ) as contexts_com_snowplowanalytics_session_identifier_2_0_0,
    array(
        select as struct
            {% if var("snowplow__custom_test", false) %}
                json_extract_scalar(json_array, '$.contents') as contents
            {% else %} null as contents
            {% endif %}
        from unnest(contexts_com_snowplowanalytics_custom_entity_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_custom_entity_1_0_0,

from prep
