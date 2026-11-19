select stock_code,min(description) description from {{ ref('stg_valid_lines') }} group by stock_code
