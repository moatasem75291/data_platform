
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select 1 as failure
where (select count(*) from "warehouse"."staging"."raw_lines") <>
 ((select count(*) from "warehouse"."candidate_a7e08816e129"."fact_invoice_line")+
 (select count(*) from "warehouse"."candidate_a7e08816e129"."stg_typed" where rejection_reason is not null))
  
  
      
    ) dbt_internal_test