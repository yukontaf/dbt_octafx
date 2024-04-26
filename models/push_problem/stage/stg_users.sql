select
    user_id,
    country_code,
    registered_dt
from
    {{ source('wh_raw', 'users') }}
where registered_dt >= '2024-01-01'
