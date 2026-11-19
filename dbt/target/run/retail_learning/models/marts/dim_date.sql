
  
    

  create  table "warehouse"."candidate_a7e08816e129"."dim_date__dbt_tmp"
  
  
    as
  
  (
    select invoice_date,extract(year from invoice_date)::int as calendar_year,extract(month from invoice_date)::int as calendar_month
from "warehouse"."candidate_a7e08816e129"."stg_valid_lines" group by invoice_date
  );
  