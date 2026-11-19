
    
    

select
    customer_key as unique_field,
    count(*) as n_records

from "warehouse"."candidate_6cd0871b5088"."dim_customer"
where customer_key is not null
group by customer_key
having count(*) > 1


