with
    source as (

        select *
        from {{ source("amplitude", "events_octa_raw_deposit_payment_system_select") }}

    ),

    renamed as (

        select
            user_id,
            time,
            device_id,
            app,
            ip,
            carrier,
            language,
            adid,
            platform,
            os_version,
            app_version,
            full_app_version,
            device_brand,
            device_manufacturer,
            device_model,
            payment_system_id,
            payment_system_slug,
            payment_system_name

        from source

    )

select *
from renamed
