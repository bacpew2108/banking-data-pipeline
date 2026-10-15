/*
    mart_card - Phân tích theo loại thẻ, thương hiệu thẻ
    Nguồn: fact_transaction, dim_card
    Mục 3.5 Banking_doc.md: "Phân tích theo loại thẻ, thương hiệu thẻ"
*/

with fact as (
    select * from {{ ref('fact_transaction') }}
),

dim_card as (
    select * from {{ ref('dim_card') }}
)

select
    dca.card_brand,
    dca.card_type,
    dca.has_chip,
    count(*)                                                    as total_transactions,
    count(*) filter (where f.is_refund)                         as refund_count,
    -- total_amount / avg_amount: chỉ tính giao dịch mua; hoàn tiền tách riêng
    coalesce(sum(f.amount) filter (where not f.is_refund), 0)   as total_amount,
    avg(f.amount) filter (where not f.is_refund)                as avg_amount,
    coalesce(-sum(f.amount) filter (where f.is_refund), 0)      as refund_amount,
    sum(f.amount)                                               as net_amount,
    count(distinct f.customer_key)                              as unique_customers,
    sum(case when f.is_error then 1 else 0 end)                 as error_count,
    round(
        sum(case when f.is_error then 1 else 0 end)::numeric
        / nullif(count(*), 0) * 100, 2
    )                                                           as error_rate_pct,
    avg(dca.credit_limit)                                       as avg_credit_limit
from fact f
inner join dim_card dca
    on f.card_key = dca.card_key
group by
    dca.card_brand, dca.card_type, dca.has_chip
