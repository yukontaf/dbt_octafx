{{ config(materialized='table') }}

SELECT {{ get_user_id_based_on_client_id("69c10177-be79-4353-826b-a56b45cd6980") }} as user_id