
  
    

  create  table "warehouse"."candidate_a7e08816e129"."fact_invoice_line__dbt_tmp"
  
  
    as
  
  (
    select * from "warehouse"."candidate_a7e08816e129"."stg_valid_lines"
  );
  