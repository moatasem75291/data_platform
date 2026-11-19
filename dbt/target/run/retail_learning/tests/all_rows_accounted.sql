
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select 1 as failure
where (select count(*) from "warehouse"."staging"."raw_lines") <>
 ((select count(*) from "warehouse"."candidate_6cd0871b5088"."fact_invoice_line")+
 (select count(*) from "warehouse"."candidate_6cd0871b5088"."stg_typed" where rejection_reason is not null))
  
  
      
    ) dbt_internal_test