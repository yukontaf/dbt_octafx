select
    u.*,
    af.appsflyer_id,
    af.install_time_dt
from {{ ref('stg_users') }} as u
inner join
    {{ ref('int_af_id') }} as af
    on u.user_id = af.user_id
