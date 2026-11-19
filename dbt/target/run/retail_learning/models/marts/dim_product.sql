
  
    

  create  table "warehouse"."candidate_a7e08816e129"."dim_product__dbt_tmp"
  
  
    as
  
  (
    select stock_code,min(description) description from "warehouse"."candidate_a7e08816e129"."stg_valid_lines" group by stock_code
  );
  