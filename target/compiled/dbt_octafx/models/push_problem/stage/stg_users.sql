select
    user_id,
    country_code,
    registered_dt
from
    `analytics-147612`.`wh_raw`.`users`
where registered_dt >= '2024-01-01'