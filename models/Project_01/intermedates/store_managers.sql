{{
    config(
        materialized='table', transient= false
    )
}}
WITH STORE_MANAGER AS (
    SELECT
        STORE_NAME,
        A.STORE_ID,
        STATE,
        ZIP_CODE,
        PRODUCT_ID,
        BRAND_ID,
        MANAGER_ID,
        FIRST_NAME,
        LAST_NAME,
        B.STAFF_ID
    FROM {{ ref('stores') }} A
    LEFT JOIN {{ source('SALES', 'STAFFS') }} B ON A.STORE_ID = B.STORE_ID
)

SELECT
    STORE_NAME,
    STATE,
    ZIP_CODE,
    A.PRODUCT_ID,
    BRAND_ID,
    MANAGER_ID,
    FIRST_NAME||'_'||LAST_NAME AS  STAFFS_NAME,
    A.STAFF_ID,
    B.ORDER_DATE,
    B.REQUIRED_DATE,
    B.SHIPPED_DATE,
    B.STORE_ID,
    B.ORDER_ID
FROM STORE_MANAGER A
LEFT JOIN {{ source('SALES', 'ORDERS') }} B ON B.STAFF_ID = A.STAFF_ID
WHERE MANAGER_ID IS NOT NULL 
