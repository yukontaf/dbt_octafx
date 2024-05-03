select *
from `analytics-147612`.`dev_gsokolov`.`stg_pushes`
where status = 'failed' and error = 'NotRegistered'