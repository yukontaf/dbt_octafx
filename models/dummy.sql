{{ config(materialized="view") }}

{{ codegen.generate_source('amplitude') }}