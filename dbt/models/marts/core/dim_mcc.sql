/*
    dim_mcc - Bảng chiều Mã ngành nghề (Merchant Category Code)
    Nguồn: staging.stg_mcc (từ silver/mcc_clean, bảng tĩnh)
    Mục 3.5 Banking_doc.md
*/

select
    row_number() over (order by mcc_id) as mcc_key,
    mcc_id,
    description
from {{ source('staging', 'stg_mcc') }}
where mcc_id is not null
