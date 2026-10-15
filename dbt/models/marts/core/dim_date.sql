/*
    dim_date - Bảng chiều Ngày (Calendar Dimension)
    Tự sinh lịch từ 2019-01-01 đến 2030-12-31
    Mục 3.5 Banking_doc.md
*/

with date_spine as (
    select
        generate_series(
            '2019-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as full_date
)

select
    cast(to_char(full_date, 'YYYYMMDD') as int) as date_key,
    full_date,
    to_char(full_date, 'Day')                   as day_of_week,
    extract(month from full_date)::int           as month,
    extract(quarter from full_date)::int         as quarter,
    extract(year from full_date)::int            as year,
    case
        when extract(dow from full_date) in (0, 6) then true
        else false
    end                                          as is_weekend
from date_spine
