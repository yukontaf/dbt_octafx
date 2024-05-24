{% macro get_user_id_based_on_client_id(client_id) %}
    {% set query %}
        SELECT user_id
        FROM {{ ref('users_cids_all') }}
        WHERE client_id = '{{ client_id }}'
        LIMIT 1
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set user_id = results.columns[0].values()[0] %} {{ return(user_id) }}
    {% endif %}
{% endmacro %}
