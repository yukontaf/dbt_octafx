with
    deals_this_month as (
        select distinct user_id
        from `analytics-147612`.`dev_gsokolov`.`trading_real_raw`
    ),

    user_accounts as (
        select distinct dtm.user_id
        from deals_this_month dtm
        left join
            `analytics-147612`.`wh_raw`.`trading_otr_accounts_real` toa
            on dtm.user_id = toa.user_id
        where toa.id is null
    )

select *
from user_accounts
