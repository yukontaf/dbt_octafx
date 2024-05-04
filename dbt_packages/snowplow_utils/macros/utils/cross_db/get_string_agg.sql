{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
  Takes care of harmonising cross-db list_agg, string_agg type functions.
#}
{%- macro get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) -%}

    {{
        return(
            adapter.dispatch("get_string_agg", "snowplow_utils")(
                base_column,
                column_prefix,
                separator,
                order_by_column,
                sort_numeric,
                order_by_column_prefix,
                is_distinct,
                order_desc,
            )
        )
    }}

{%- endmacro -%}

{% macro default__get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) %}

    {% if (
        base_column != order_by_column
        or column_prefix != order_by_column_prefix
        or sort_numeric
    ) and is_distinct %}
        {%- do exceptions.raise_compiler_error(
            "Snowplow Error: "
            ~ target.type
            ~ " does not support distinct with a different ordering column, or when the order column is numeric."
        ) -%}
    {% endif %}

    listagg(
        {% if is_distinct %}
            distinct
        {% endif %} {{ column_prefix }}.{{ base_column }}::varchar,
        '{{separator}}'
    ) within group (
        order by

            {% if sort_numeric -%}
                to_numeric({{ order_by_column_prefix }}.{{ order_by_column }}, 38, 9)
                {% if order_desc %} desc {% endif %}

            {% else %}
                {{ order_by_column_prefix }}.{{ order_by_column }}::varchar
                {% if order_desc %} desc
                {% endif %}

            {%- endif -%}
    )

{% endmacro %}

{% macro bigquery__get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) %}

    {% if (
        base_column != order_by_column
        or column_prefix != order_by_column_prefix
        or sort_numeric
    ) and is_distinct %}
        {%- do exceptions.raise_compiler_error(
            "Snowplow Error: "
            ~ target.type
            ~ " does not support distinct with a different ordering column, or when the order column is numeric."
        ) -%}
    {% endif %}

    string_agg(
        {% if is_distinct %}
            distinct {% endif %} cast({{ column_prefix }}.{{ base_column }} as string),
        '{{separator}}'
        order by

            {% if sort_numeric -%}
                cast({{ order_by_column_prefix }}.{{ order_by_column }} as numeric)
                {% if order_desc %} desc {% endif %}

            {% else %}
                cast({{ order_by_column_prefix }}.{{ order_by_column }} as string)
                {% if order_desc %} desc
                {% endif %}

            {%- endif -%}
    )

{% endmacro %}


{% macro postgres__get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) %}

    {% if (
        base_column != order_by_column
        or column_prefix != order_by_column_prefix
        or sort_numeric
    ) and is_distinct %}
        {%- do exceptions.raise_compiler_error(
            "Snowplow Error: "
            ~ target.type
            ~ " does not support distinct with a different ordering column, or when the order column is numeric."
        ) -%}
    {% endif %}

    string_agg(
        {% if is_distinct %}
            distinct
        {% endif %} {{ column_prefix }}.{{ base_column }}::varchar,
        '{{separator}}'
        order by

            {% if sort_numeric -%}
                {{ order_by_column_prefix }}.{{ order_by_column }}::decimal
                {% if order_desc %} desc {% endif %}

            {% else %}
                {{ order_by_column_prefix }}.{{ order_by_column }}::varchar
                {% if order_desc %} desc
                {% endif %}

            {%- endif -%}
    )

{% endmacro %}

{% macro redshift__get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) %}

    {% if (
        base_column != order_by_column
        or column_prefix != order_by_column_prefix
        or sort_numeric
    ) and is_distinct %}
        {%- do exceptions.raise_compiler_error(
            "Snowplow Error: "
            ~ target.type
            ~ " does not support distinct with a different ordering column, or when the order column is numeric."
        ) -%}
    {% endif %}

    listagg(
        {% if is_distinct %}
            distinct
        {% endif %} {{ column_prefix }}.{{ base_column }}::varchar,
        '{{separator}}'
    ) within group (
        order by

            {% if sort_numeric -%}
                text_to_numeric_alt(
                    {{ order_by_column_prefix }}.{{ order_by_column }}, 38, 9
                )
                {% if order_desc %} desc {% endif %}

            {% else %}
                {{ order_by_column_prefix }}.{{ order_by_column }}::varchar
                {% if order_desc %} desc
                {% endif %}

            {%- endif -%}
    )

{% endmacro %}

{% macro spark__get_string_agg(
    base_column,
    column_prefix,
    separator=",",
    order_by_column=base_column,
    sort_numeric=false,
    order_by_column_prefix=column_prefix,
    is_distinct=false,
    order_desc=false
) %}
    /* Explaining inside out:
  1. Create a group array which is made of sub-arrays of the base_column and the sort column
  2. Sort these sub-arrays based on a lamdba function that compares on the second element (the sort column, casted if needed)
  3. Use transform to select just the first element of the array
  4. Optionally use array_distinct
  5. Join the array into a string
  */
    array_join(
        {% if is_distinct %}
            array_distinct(
        {% endif %}
            transform(
                array_sort(
                    filter (
                        collect_list(
                            array(
                                {{ column_prefix }}.{{ base_column }}::string,
                                {{ order_by_column_prefix }}.{{ order_by_column }}
                                ::string
                            )
                        ),
                        x -> x[0] is not null
                    ),
                    (left, right) ->

                    {%- if sort_numeric -%}
                        case
                            when
                                cast(left[1] as numeric(38, 9))
                                {% if order_desc %} >
                                {% else %} <
                                {% endif %} cast(right[1] as numeric(38, 9))
                            then -1
                            when
                                cast(left[1] as numeric(38, 9))
                                {% if order_desc %} <
                                {% else %} >
                                {% endif %} cast(right[1] as numeric(38, 9))
                            then 1
                            else 0
                        end

                    {% else %}
                        case
                            when
                                left[1]
                                {% if order_desc %} >
                                {% else %} <
                                {% endif %} right[1]
                            then -1
                            when
                                left[1]
                                {% if order_desc %} <
                                {% else %} >
                                {% endif %} right[1]
                            then 1
                            else 0
                        end

                    {% endif %}
                ),
                x -> x[0]
            )
        {% if is_distinct %}) {% endif %},
        '{{separator}}'
    )

{% endmacro %}
