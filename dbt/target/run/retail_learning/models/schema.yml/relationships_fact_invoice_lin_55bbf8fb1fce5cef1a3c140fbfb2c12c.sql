
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select customer_key as from_field
    from "warehouse"."candidate_6cd0871b5088"."fact_invoice_line"
    where customer_key is not null
),

parent as (
    select customer_key as to_field
    from "warehouse"."candidate_6cd0871b5088"."dim_customer"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test