
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select 1 as failure
where (select sum(line_amount) from "warehouse"."candidate_a7e08816e129"."fact_invoice_line")
 is distinct from (select sum(net_sales) from "warehouse"."candidate_a7e08816e129"."daily_sales")
  
  
      
    ) dbt_internal_test