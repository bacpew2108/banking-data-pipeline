/*
    dim_customer - Bảng chiều Khách hàng
    Nguồn: staging.stg_users (từ silver/users_clean)
    Mục 3.5 Banking_doc.md
*/

with deduplicated as (
    -- Silver lưu mọi phiên bản CDC của một id: lấy phiên bản mới nhất theo _cdc_timestamp
    select distinct on (id)
        id as client_id,
        current_age,
        gender,
        yearly_income,
        credit_score,
        address
    from {{ source('staging', 'stg_users') }}
    where id is not null
    order by id, _cdc_timestamp desc nulls last
)

select
    row_number() over (order by client_id) as customer_key,
    client_id,
    current_age,
    gender,
    yearly_income,
    credit_score,
    address
from deduplicated

