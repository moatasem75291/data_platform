select invoice_date,extract(year from invoice_date)::int as calendar_year,extract(month from invoice_date)::int as calendar_month
from {{ ref('stg_valid_lines') }} group by invoice_date
