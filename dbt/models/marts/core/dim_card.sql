/*
    dim_card - Bảng chiều Thẻ
    Nguồn: staging.stg_cards (từ silver/cards_clean, đã mask PII)
    Mục 3.5 Banking_doc.md
    Lưu ý: card_number đã bị drop ở Silver, chỉ còn card_number_hash & card_number_masked
*/

with deduplicated as (
    -- Silver lưu mọi phiên bản CDC của một id: lấy phiên bản mới nhất theo _cdc_timestamp
    select distinct on (id)
        id as card_id,
        card_brand,
        card_type,
        has_chip,
        credit_limit
    from {{ source('staging', 'stg_cards') }}
    where id is not null
    order by id, _cdc_timestamp desc nulls last
)

select
    row_number() over (order by card_id) as card_key,
    card_id,
    card_brand,
    card_type,
    has_chip,
    credit_limit
from deduplicated

