
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    source_row_id as unique_field,
    count(*) as n_records

from "warehouse"."candidate_a7e08816e129"."fact_invoice_line"
where source_row_id is not null
group by source_row_id
having count(*) > 1



  
  
      
    ) dbt_internal_test