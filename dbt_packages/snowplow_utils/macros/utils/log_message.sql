{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro log_message(message, is_printed=var("snowplow__has_log_enabled", true)) %}
    {{ log(dbt_utils.pretty_log_format(message), info=is_printed) }}
{% endmacro %}
