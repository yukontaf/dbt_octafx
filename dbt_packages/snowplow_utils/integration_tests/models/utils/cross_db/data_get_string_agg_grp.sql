{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
with
    data as (

        select 'a' as string_col, 'y' as group_col

        union all

        select 'b' as string_col, 'y' as group_col

        union all

        select 'c' as string_col, 'z' as group_col

        union all

        select 'd' as string_col, 'z' as group_col

    )

select *
from data
