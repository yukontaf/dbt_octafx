with
    source as (

        select * from {{ source("amplitude", "events_octa_raw_filled_personal_info") }}

    ),

    renamed as (

        select
            event_type,
            event_id,
            device_id,
            timestamp,
            user_id,
            appsflyer_id,
            client_id,
            app,
            app_version,
            os_version,
            os_name,
            platform,
            device_type,
            device_brand,
            device_manufacturer,
            device_model,
            browser,
            browser_version,
            carrier,
            idfa,
            idfv,
            adid

        from source

    )

select *
from renamed
