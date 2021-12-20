{{
    config(
        materialized='table', transient= false
    )
}}
with CATEGORIES as (
  select 
    A.*, 
    C.CATEGORY_NAME 
  from 
    {{ source('PRODUCTION', 'PRODUCTS') }} A
    left join {{ source('PRODUCTION', 'BRANDS') }} B on B.brand_id = A.brand_id 
    left join {{ source('PRODUCTION', 'CATEGORIES') }} C on C.CATEGORY_ID = A.CATEGORY_ID 
  group by 
    1, 
    2, 
    3, 
    4, 
    5, 
    6, 
    7
)

SELECT * FROM CATEGORIES
