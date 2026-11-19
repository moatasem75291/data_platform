select customer_key from {{ ref('stg_valid_lines') }} group by customer_key
