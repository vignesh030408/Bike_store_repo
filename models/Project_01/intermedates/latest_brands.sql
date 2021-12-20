{{
    config(
        materialized='table', transient= false
    )
}}
with latest_brands as (
    SELECT 
        PRODUCT_NAME,
        BRAND_ID,
        CATEGORY_ID,
        MODEL_YEAR,
        LIST_PRICE,
        CATEGORY_NAME,
        D.store_id,
        D.product_id,
        D.quantity
    FROM {{ source('PRODUCTION', 'STOCKS') }} D
    LEFT JOIN {{ ref('Categories') }} E on E.PRODUCT_ID = D.PRODUCT_ID
)

SELECT 
    PRODUCT_NAME,
    BRAND_ID,
    CATEGORY_ID,
    LIST_PRICE,
    CATEGORY_NAME,
    STORE_ID,
    PRODUCT_ID,
    QUANTITY
FROM latest_brands
WHERE MODEL_YEAR > 2017
LIMIT 1000