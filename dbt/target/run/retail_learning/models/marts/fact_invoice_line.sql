
  
    

  create  table "warehouse"."candidate_6cd0871b5088"."fact_invoice_line__dbt_tmp"
  
  
    as
  
  (
    select * from "warehouse"."candidate_6cd0871b5088"."stg_valid_lines"
  );
  