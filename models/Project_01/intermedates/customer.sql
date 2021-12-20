{{
    config(
        materialized='table', transient= false
    )
}}
WITH CUSTOMERS AS (
    SELECT
        A.ORDER_ID,
        A.STORE_ID,
        A.STAFF_ID,
        c.FIRST_NAME||'_'||LAST_NAME AS CUSTOMERS_NAME,
        C.EMAIL,
        C.STREET,
        C.CITY,
        C.STATE,
        C.ZIP_CODE,
        D.DISCOUNT
    FROM {{ ref('store_managers') }} A 
    LEFT JOIN {{ source('SALES', 'ORDERS') }} B ON B.ORDER_ID = A.ORDER_ID
    LEFT JOIN {{ source('SALES', 'CUSTOMERS') }} C ON B.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN {{ source('SALES', 'ORDER_ITEMS') }} D ON D.ORDER_ID = A.ORDER_ID
)

SELECT * FROM CUSTOMERS