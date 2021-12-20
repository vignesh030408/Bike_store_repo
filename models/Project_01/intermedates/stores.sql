{{
    config(
        materialized='table', transient= false
    )
}}
with STORE as (
    SELECT 
        Brand_id,
        product_id,
        quantity,
        B.store_id,
        A.store_name,
        A.STATE,
        A.ZIP_CODE
    FROM  {{ source('SALES', 'STORES') }}  A
    LEFT JOIN {{ ref('latest_brands') }} B ON B.store_id = A.store_id
)

SELECT
    *
    FROM STORE
    GROUP BY 1,2,3,4,5,6,7