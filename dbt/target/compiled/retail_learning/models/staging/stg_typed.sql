with parsed as (
 select *,
   case when pg_input_is_valid(quantity, 'numeric') then quantity::numeric(20,6) end q,
   case when pg_input_is_valid(unit_price, 'numeric') then unit_price::numeric(20,6) end p,
   case when pg_input_is_valid(invoice_date, 'timestamp') then invoice_date::timestamp end local_ts
 from "warehouse"."staging"."raw_lines"
)
select *, case
 when invoice is null or trim(invoice)='' then 'missing_invoice'
 when stock_code is null or trim(stock_code)='' then 'missing_stock_code'
 when local_ts is null then 'invalid_date'
 when q is null or q=0 or q<>floor(q) or abs(q)>1000000000 then 'invalid_quantity'
 when p is null or p<0 then 'invalid_price'
 else null end rejection_reason
from parsed