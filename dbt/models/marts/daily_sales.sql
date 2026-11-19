select invoice_date,currency,country,count(*) line_count,
 sum(case when line_amount>0 and not is_cancellation then line_amount else 0 end)::numeric(38,6) gross_sales,
 sum(case when line_amount<0 then -line_amount else 0 end)::numeric(38,6) credits,
 sum(line_amount)::numeric(38,6) net_sales
from {{ ref('fact_invoice_line') }} group by invoice_date,currency,country
