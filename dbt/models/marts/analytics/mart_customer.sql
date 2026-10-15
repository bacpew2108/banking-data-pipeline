/*
    mart_customer - Hành vi chi tiêu theo khách hàng
    Nguồn: fact_transaction, dim_customer
    Mục 3.5 Banking_doc.md: "Hành vi chi tiêu theo khách hàng (tổng chi tiêu, tần suất giao dịch)"
*/

with fact as (
    select * from {{ ref('fact_transaction') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
)

select
    dc.customer_key,
    dc.client_id,
    dc.current_age,
    dc.gender,
    dc.yearly_income,
    dc.credit_score,
    count(*)                                                    as total_transactions,
    count(*) filter (where f.is_refund)                         as refund_count,
    -- Chi tiêu chỉ tính giao dịch mua; hoàn tiền tách riêng, net_spending = mua - hoàn
    coalesce(sum(f.amount) filter (where not f.is_refund), 0)   as total_spending,
    coalesce(-sum(f.amount) filter (where f.is_refund), 0)      as total_refund,
    sum(f.amount)                                               as net_spending,
    avg(f.amount) filter (where not f.is_refund)                as avg_transaction_amount,
    min(f.amount) filter (where not f.is_refund)                as min_transaction_amount,
    max(f.amount) filter (where not f.is_refund)                as max_transaction_amount,
    sum(case when f.is_error then 1 else 0 end)                 as error_count,
    round(
        sum(case when f.is_error then 1 else 0 end)::numeric
        / nullif(count(*), 0) * 100, 2
    )                                                           as error_rate_pct
from fact f
inner join dim_customer dc
    on f.customer_key = dc.customer_key
group by
    dc.customer_key, dc.client_id, dc.current_age,
    dc.gender, dc.yearly_income, dc.credit_score
