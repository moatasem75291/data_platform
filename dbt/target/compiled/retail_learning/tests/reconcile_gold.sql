select 1 as failure
where (select sum(line_amount) from "warehouse"."candidate_6cd0871b5088"."fact_invoice_line")
 is distinct from (select sum(net_sales) from "warehouse"."candidate_6cd0871b5088"."daily_sales")