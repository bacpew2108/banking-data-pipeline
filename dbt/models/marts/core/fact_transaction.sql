/*
    fact_transaction - Bảng Fact giao dịch (Star Schema)
    Nguồn: staging.stg_transactions JOIN với 4 bảng Dimension
    Mục 3.5 Banking_doc.md
*/

with transactions as (
    -- Mỗi giao dịch đúng 1 dòng, kể cả khi staging lỡ chứa bản ghi trùng
    select distinct on (id) *
    from {{ source('staging', 'stg_transactions') }}
    where id is not null
    order by id
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

dim_card as (
    select * from {{ ref('dim_card') }}
),

dim_mcc as (
    select * from {{ ref('dim_mcc') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    row_number() over (order by t.id)                       as transaction_key,
    t.id                                                     as transaction_id,
    coalesce(dc.customer_key, -1)                           as customer_key,
    coalesce(dca.card_key, -1)                              as card_key,
    coalesce(dm.mcc_key, -1)                                as mcc_key,
    coalesce(dd.date_key, cast(to_char(t.tx_timestamp::date, 'YYYYMMDD') as int)) as date_key,
    t.amount,
    coalesce(t.is_refund, t.amount < 0)                     as is_refund,
    coalesce(t.is_error, false)                             as is_error,
    t.use_chip,
    t.merchant_city,
    t.merchant_state
from transactions t
left join dim_customer dc
    on t.client_id = dc.client_id
left join dim_card dca
    on t.card_id = dca.card_id
left join dim_mcc dm
    on t.mcc = dm.mcc_id
left join dim_date dd
    on t.tx_timestamp::date = dd.full_date
