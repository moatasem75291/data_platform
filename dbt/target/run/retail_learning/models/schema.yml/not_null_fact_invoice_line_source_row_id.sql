
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_row_id
from "warehouse"."candidate_6cd0871b5088"."fact_invoice_line"
where source_row_id is null



  
  
      
    ) dbt_internal_test