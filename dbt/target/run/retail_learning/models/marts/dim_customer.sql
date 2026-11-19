
  
    

  create  table "warehouse"."candidate_a7e08816e129"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    select customer_key from "warehouse"."candidate_a7e08816e129"."stg_valid_lines" group by customer_key
  );
  