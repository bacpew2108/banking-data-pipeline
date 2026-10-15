/*
    mart_daily_transaction - Tổng hợp giao dịch theo ngày
    Nguồn: fact_transaction, dim_date
    Mục 3.5 Banking_doc.md: "Tổng hợp giao dịch theo ngày (số lượng, tổng giá trị, tỷ lệ lỗi)"
*/

with fact as (
    select * from {{ ref('fact_transaction') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    dd.date_key,
    dd.full_date,
    dd.day_of_week,
    dd.month,
    dd.quarter,
    dd.year,
    dd.is_weekend,
    count(*)                                                    as total_transactions,
    count(*) filter (where not f.is_refund)                     as purchase_count,
    count(*) filter (where f.is_refund)                         as refund_count,
    -- total_amount / avg_amount: chỉ tính giao dịch mua (doanh số), không trừ hoàn tiền
    coalesce(sum(f.amount) filter (where not f.is_refund), 0)   as total_amount,
    avg(f.amount) filter (where not f.is_refund)                as avg_amount,
    coalesce(-sum(f.amount) filter (where f.is_refund), 0)      as refund_amount,
    sum(f.amount)                                               as net_amount,
    sum(case when f.is_error then 1 else 0 end)                 as error_count,
    round(
        sum(case when f.is_error then 1 else 0 end)::numeric
        / nullif(count(*), 0) * 100, 2
    )                                                           as error_rate_pct
from fact f
inner join dim_date dd
    on f.date_key = dd.date_key
group by
    dd.date_key, dd.full_date, dd.day_of_week,
    dd.month, dd.quarter, dd.year, dd.is_weekend
order by dd.date_key
