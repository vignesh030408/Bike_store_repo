{{
    config(
        materialized='table', transient= false
    )
}}
with purchase as (
    SELECT
        A.product_name,
        A.brand_id,
        A.category_name,
        B.store_id,
        C.quantity,
        C.store_name,
        D.staffs_name,
        D.order_date,
        D.REQUIRED_DATE,
        D.SHIPPED_DATE,
        D.Manager_id,
        D.store_id
    FROM {{ ref('Categories') }} A 
    LEFT JOIN {{ ref('latest_brands') }} B ON A.PRODUCT_ID = B.PRODUCT_ID 
    LEFT JOIN {{ ref('stores') }} C ON C.PRODUCT_ID = B.PRODUCT_ID
    LEFT JOIN {{ ref('store_managers') }} D ON D.PRODUCT_ID = C.PRODUCT_ID

)
SELECT * FROM purchase
