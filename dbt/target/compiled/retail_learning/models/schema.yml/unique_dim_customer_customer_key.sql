
    
    

select
    customer_key as unique_field,
    count(*) as n_records

from "warehouse"."candidate_a7e08816e129"."dim_customer"
where customer_key is not null
group by customer_key
having count(*) > 1


