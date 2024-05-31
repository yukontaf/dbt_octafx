with
    first_deals as (
        select user_id, min(open_time_dt) as first_deal_date
        from {{ ref("trading_real_raw") }}
        where open_time_dt >= "2024-05-01" and cmd < 2
        group by user_id
    ),
    deals_within_two_weeks as (
        select rr.user_id, count(close_time_dt) as deal_count
        from {{ ref("trading_real_raw") }} rr
        right join {{ ref("tw_segment") }} ts on rr.user_id = ts.user_id
        inner join first_deals fd on rr.user_id = fd.user_id
        where
            rr.open_time_dt
            between fd.first_deal_date and fd.first_deal_date + interval '14' day
            and rr.open_time_dt >= "2024-05-01"
            and rr.cmd < 2
        group by rr.user_id
    )
select user_id, deal_count
from deals_within_two_weeks
order by deal_count desc


