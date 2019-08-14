with l12m_txn_amount_per_account as (
select
a.id
, round(sum(c.amount * 0.01 / p.paid_currency_exchange_rate),2) as l12m_txn_amount_usd -- convert amount from cents to dollars
from mongo_account a
join mongo_payment p
    on a.id = p.account_id
    and p.charge_id != 'Manual'
    and p.exclude_from_reports = 0
    and p.active_status = 'active'
    and p.charge_id is not null
    and (
        p.payment_charge_date between dateadd(day, -365, a.subscription_canceled) and a.subscription_canceled -- if account cancelled, aggregate payments in the last 12 months prior to cancel
        OR
        p.payment_charge_date >= dateadd(day, -365, getdate()) -- otherwise, aggregate payments in the last 12 months to date
        )
join stripe_charge c
    on c.id = p.charge_id
    and c.captured = 1
group by 1
),

temp as (
select
a.id
, a.days_since_first_paid
, a.total_paid
, l12m_txn_amount_usd
, CASE
    WHEN subscription_canceled is NULL AND days_since_first_paid > 0 THEN days_since_first_paid
    WHEN subscription_canceled is not NULL AND DATEDIFF('day',first_paid, subscription_canceled) <= 0 THEN 1
    ELSE DATEDIFF(day,first_paid, subscription_canceled)
        END as atv_segment_calculation_period
--, CASE WHEN days_since_first_paid > 0 THEN CAST ((total_paid * 365.0)/atv_segment_calculation_period AS double precision) END as atv -- old logic that looks at total txn amount / total days since first paid
, 150 as segments_maturation_period
, CASE WHEN days_since_first_paid > 0 then round((l12m_txn_amount_usd / least(atv_segment_calculation_period, 365)) * 365) END as atv -- new logic that looks at latest 12 month txn amount for accounts older than 1 yr
, CASE
    WHEN days_since_first_paid IS NULL THEN 'Not Paid Post Plan'
    WHEN days_since_first_paid < segments_maturation_period then 'Insufficient Data'
    WHEN atv < 20000 then 'Less than 20K'
    WHEN atv >= 20000 AND atv < 50000 then '20K - 50K'
    WHEN atv >= 50000 AND atv < 200000 then '50K - 200K'
    WHEN atv >= 200000 then '200K+'
    ELSE 'Unknown'
        END as member_segment_atv

from mongo_account a
left join l12m_txn_amount_per_account l12m_txn
    on a.id = l12m_txn.id
)

select member_segment_atv, count(*), count(distinct id) from temp group by 1 order by 3 desc
;