
  
    

  create  table "warehouse"."candidate_6cd0871b5088"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    select customer_key from "warehouse"."candidate_6cd0871b5088"."stg_valid_lines" group by customer_key
  );
  