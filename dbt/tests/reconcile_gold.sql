select 1 as failure
where (select sum(line_amount) from {{ ref('fact_invoice_line') }})
 is distinct from (select sum(net_sales) from {{ ref('daily_sales') }})
