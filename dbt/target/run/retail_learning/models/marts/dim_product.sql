
  
    

  create  table "warehouse"."candidate_6cd0871b5088"."dim_product__dbt_tmp"
  
  
    as
  
  (
    select stock_code,min(description) description from "warehouse"."candidate_6cd0871b5088"."stg_valid_lines" group by stock_code
  );
  