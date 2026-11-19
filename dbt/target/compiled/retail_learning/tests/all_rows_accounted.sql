select 1 as failure
where (select count(*) from "warehouse"."staging"."raw_lines") <>
 ((select count(*) from "warehouse"."candidate_a7e08816e129"."fact_invoice_line")+
 (select count(*) from "warehouse"."candidate_a7e08816e129"."stg_typed" where rejection_reason is not null))