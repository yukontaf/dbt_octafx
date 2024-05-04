{#
  Copyright 2023 yu-iskw

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#}
{% macro assert_true(value) %}
    {% if value is boolean and value is not true %}
        {% do exceptions.raise_compiler_error(
            "FAILED: value " ~ value ~ " is not true."
        ) %}
    {% else %} {% do log("SUCCESS") %}
    {% endif %}
{% endmacro %}

{% macro assert_false(value) %}
    {% if value is boolean and value is not false %}
        {% do exceptions.raise_compiler_error(
            "FAILED: value " ~ value ~ " is not false."
        ) %}
    {% else %} {% do log("SUCCESS") %}
    {% endif %}
{% endmacro %}

{% macro assert_equals(value, expected_value) %}
    {% if value != expected_value %}
        {% do exceptions.raise_compiler_error(
            "FAILED: value " ~ value ~ " does not equal to " ~ expected_value
        ) %}
    {% else %} {% do log("SUCCESS") %}
    {% endif %}
{% endmacro %}

{% macro assert_str_in_value(str, value) %}
    {% if str not in value %}
        {% do exceptions.raise_compiler_error(
            "FAILED: the string " ~ str ~ " was not found in " ~ value
        ) %}
    {% else %} {% do log("SUCCESS") %}
    {% endif %}
{% endmacro %}

{% macro assert_element_in_list(element, list_values) %}
    {% if element not in list_values %}
        {% do exceptions.raise_compiler_error(
            "FAILED: the element "
            ~ element
            ~ " was not found in "
            ~ list_values
        ) %}
    {% else %} {% do log("SUCCESS") %}
    {% endif %}
{% endmacro %}

{% macro assert_dict_equals(value, expected_value) %}
    {% for k, v in value.items() %}
        {% if k not in expected_value.keys() %}
            {% do exceptions.raise_compiler_error(
                "FAILED: key "
                ~ k
                ~ " from "
                ~ value
                ~ " does not exist in "
                ~ expected_value
            ) %}
        {% elif v is mapping and expected_value[k] is mapping %}
            {% do assert_dict_equals(v, expected_value[k]) %}
        {% elif v != expected_value[k] %}
            {% do exceptions.raise_compiler_error(
                "FAILED: value "
                ~ x
                ~ " from "
                ~ value
                ~ " does not equal to "
                ~ expected_value[k]
            ) %}
        {% endif %}
    {% endfor %}

    {% do log("SUCCESS") %}
{% endmacro %}
