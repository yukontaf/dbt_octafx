select *
from {{ref('stg_pushes')}}
where status = 'failed' and error = 'NotRegistered'

