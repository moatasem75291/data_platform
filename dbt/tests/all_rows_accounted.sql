select 1 as failure
where (select count(*) from {{ source('retail_input', 'raw_lines') }}) <>
 ((select count(*) from {{ ref('fact_invoice_line') }})+
 (select count(*) from {{ ref('stg_typed') }} where rejection_reason is not null))
