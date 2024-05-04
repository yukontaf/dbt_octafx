{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# This tests the output of a dummy set of inputs to the user table macro to ensure that it returns what we expect to come out does.
This doesn't run on any actual data, we are just comparing the sql that is generated - removing whitespace to allow for changes to that.
Note that we have to pass the test = true argument for this to work without having to create all the manifest and event limits table.

It runs 6 tests:
1) A single context for the user
2) 2 contexts for the user
3) Providing a custom user field
4) Custom user field from an sde
5) Custom user field from a context
6) Custom user field from an sde, but also provided a context
7) Custom user field from an sde, but also provided a context, and a user id alias
8) Custom user field from an sde, but also provided a context, and a user id alias, and flat columns

#}
{% macro test_users_table() %}

    {{
        return(
            adapter.dispatch(
                "test_users_table", "snowplow_normalize_integration_tests"
            )()
        )
    }}

{% endmacro %}

{% macro bigquery__test_users_table() %}

    {% set expected_dict = {
        "1_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "2_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field": "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_sde": "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_context": "with defined_user_id as ( select coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2) as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both": "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both_w_alias": "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as my_user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by my_user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where my_user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both_w_alias_and_flat": "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as my_user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table , app_id , network_user_id -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 from `"
        ~ target.project
        ~ "`."
        ~ target.dataset
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by my_user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where my_user_id is not null ) select * except (rn) from users_ordering where rn = 1",
    } %}

    {% set results_dict = {
        "1_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0"],
            [["contextTestId", "contextTestClass"]],
            [["string", "integer"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "2_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field": snowplow_normalize.users_table(
            "testId",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_sde": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_TEST_1_0_1",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_context": snowplow_normalize.users_table(
            "contextTestId2",
            "",
            "CONTEXTS_TEST2_1_0_5",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_TEST_1_0_1",
            "CONTEXTS_TEST2_1_0_5",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_TEST_1_0_1",
            "CONTEXTS_TEST2_1_0_5",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias_and_flat": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_TEST_1_0_1",
            "CONTEXTS_TEST2_1_0_5",
            ["CONTEXTS_TEST_1_0_0", "CONTEXTS_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            ["app_id", "network_user_id"],
            remove_new_event_check=true,
        ).split()
        | join(" "),
    } %}

    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias_and_flat'])}} #}
    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}

{% endmacro %}


{% macro databricks__test_users_table() %}

    {% set expected_dict = {
        "1_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "2_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field": "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_sde": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_context": "with defined_user_id as ( select CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1[0].test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both_w_alias": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as my_user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by my_user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where my_user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both_w_alias_and_flat": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as my_user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- Flat columns from event table , app_id , network_user_id -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from `"
        ~ target.catalog
        ~ "`."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by my_user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where my_user_id is not null ) select * except (rn) from users_ordering where rn = 1",
    } %}

    {% set results_dict = {
        "1_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0"],
            [["contextTestId", "contextTestClass"]],
            [["string", "integer"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "2_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field": snowplow_normalize.users_table(
            "testId",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_sde": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_context": snowplow_normalize.users_table(
            "testId",
            "",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias_and_flat": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            ["app_id", "network_user_id"],
            remove_new_event_check=true,
        ).split()
        | join(" "),
    } %}

    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias_and_flat'])}} #}
    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}

{% endmacro %}


{% macro snowflake__test_users_table() %}

    {% set expected_dict = {
        "1_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::string as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::integer as context_test_class from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "2_context": "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field": "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field_sde": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field_context": "with defined_user_id as ( select CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1[0]:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field_both": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field_both_w_alias": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as my_user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where my_user_id is not null qualify row_number() over (partition by my_user_id order by latest_collector_tstamp desc) = 1",
        "custom_user_field_both_w_alias_and_flat": "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as my_user_id , collector_tstamp as latest_collector_tstamp -- Flat columns from event table , app_id , network_user_id -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "
        ~ target.database
        ~ "."
        ~ target.schema
        ~ "_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where my_user_id is not null qualify row_number() over (partition by my_user_id order by latest_collector_tstamp desc) = 1",
    } %}

    {% set results_dict = {
        "1_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0"],
            [["contextTestId", "contextTestClass"]],
            [["string", "integer"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "2_context": snowplow_normalize.users_table(
            "user_id",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field": snowplow_normalize.users_table(
            "testId",
            "",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_sde": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_context": snowplow_normalize.users_table(
            "testId",
            "",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            remove_new_event_check=true,
        ).split()
        | join(" "),
        "custom_user_field_both_w_alias_and_flat": snowplow_normalize.users_table(
            "testId",
            "UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0",
            "CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0",
            ["CONTEXTS_TEST_1_0_0", "CONTEXT_TEST2_1_0_5"],
            [
                ["contextTestId", "contextTestClass"],
                ["contextTestId2", "contextTestClass2"],
            ],
            [["boolean", "string"], ["interger", "string"]],
            "my_user_id",
            ["app_id", "network_user_id"],
            remove_new_event_check=true,
        ).split()
        | join(" "),
    } %}

    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias'])}} #}
    {# {{ print(results_dict['custom_user_field_both_w_alias_and_flat'])}} #}
    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}

{% endmacro %}
