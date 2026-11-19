select source_row_id,invoice,stock_code,description,
 q::bigint quantity,p::numeric(20,6) unit_price,
 local_ts at time zone 'Europe/London' invoice_ts,
 local_ts::date invoice_date,
 coalesce(nullif(regexp_replace(trim(customer_id),'[.]0$',''),''),'UNKNOWN') customer_key,
 coalesce(country,'UNKNOWN') country,'GBP'::text currency,
 (upper(invoice) like 'C%' or q<0) is_cancellation,
 (q*p)::numeric(28,6) line_amount,
 source_sheet,source_row
from {{ ref('stg_typed') }}
where rejection_reason is null
