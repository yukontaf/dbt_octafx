{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
with
    prep as (
        select
            * except (
                contexts_test_1_0_0,
                contexts_test2_1_0_0,
                contexts_test2_1_0_1,
                contexts_test2_1_0_2,
                contexts_test2_1_0_3,
                contexts_test2_1_0_4,
                contexts_test2_1_0_5
            ),
            json_extract_array(contexts_test_1_0_0) as contexts_test_1_0_0,
            json_extract_array(contexts_test2_1_0_0) as contexts_test2_1_0_0,
            json_extract_array(contexts_test2_1_0_1) as contexts_test2_1_0_1,
            json_extract_array(contexts_test2_1_0_2) as contexts_test2_1_0_2,
            json_extract_array(contexts_test2_1_0_3) as contexts_test2_1_0_3,
            json_extract_array(contexts_test2_1_0_4) as contexts_test2_1_0_4,
            json_extract_array(contexts_test2_1_0_5) as contexts_test2_1_0_5

        from {{ ref("snowplow_norm_dummy_events") }}
    )

-- recreate repeated record field i.e. array of structs as is originally in BQ events
-- table
select
    * except (
        unstruct_event_test_1_0_0,
        unstruct_event_test_1_0_1,
        unstruct_event_test2_1_0_0,
        unstruct_event_test2_1_0_1,
        contexts_test_1_0_0,
        contexts_test2_1_0_0,
        contexts_test2_1_0_1,
        contexts_test2_1_0_2,
        contexts_test2_1_0_3,
        contexts_test2_1_0_4,
        contexts_test2_1_0_5
    ),
    -- order is reversed to test the aliasing of the coalesced columns
    struct(
        json_extract_scalar(unstruct_event_test_1_0_0, '$.test_class') as test_class,
        json_extract_scalar(unstruct_event_test_1_0_0, '$.test_id') as test_id
    ) as unstruct_event_test_1_0_0,
    struct(
        json_extract_scalar(unstruct_event_test_1_0_1, '$.test_class') as test_class,
        json_extract_scalar(unstruct_event_test_1_0_1, '$.test_id') as test_id
    ) as unstruct_event_test_1_0_1,
    struct(
        json_extract_scalar(unstruct_event_test2_1_0_0, '$.test_word') as test_word,
        json_extract_scalar(unstruct_event_test2_1_0_0, '$.test_idea') as test_idea
    ) as unstruct_event_test2_1_0_0,
    struct(
        json_extract_scalar(unstruct_event_test2_1_0_1, '$.test_word') as test_word,
        json_extract_scalar(unstruct_event_test2_1_0_1, '$.test_idea') as test_idea
    ) as unstruct_event_test2_1_0_1,
    array(
        select
            struct(
                json_extract_scalar(json_array, '$.context_test_id') as context_test_id,
                json_extract_scalar(
                    json_array, '$.context_test_class'
                ) as context_test_class
            )
        from unnest(contexts_test_1_0_0) as json_array
    ) as contexts_test_1_0_0,
    -- order is reversed to test the aliasing of the coalesced columns
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_0) as json_array
    ) as contexts_test2_1_0_0,
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_1) as json_array
    ) as contexts_test2_1_0_1,
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_2) as json_array
    ) as contexts_test2_1_0_2,
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_3) as json_array
    ) as contexts_test2_1_0_3,
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_4) as json_array
    ) as contexts_test2_1_0_4,
    array(
        select
            struct(
                json_extract_scalar(
                    json_array, '$.context_test_class2'
                ) as context_test_class2,
                json_extract_scalar(
                    json_array, '$.context_test_id2'
                ) as context_test_id2
            )
        from unnest(contexts_test2_1_0_5) as json_array
    ) as contexts_test2_1_0_5

from prep
