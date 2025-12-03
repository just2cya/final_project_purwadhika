{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE(a.created_date, 'Asia/Jakarta') AS order_date_WIB,
    a.country,
    b.category,
    count(a.order_id) AS order_count,
    sum(a.quantity) AS quantity,
    sum(a.amount) AS amount
  FROM {{ source('justicia_finpro_gold', 'fact_orders') }} a 
    inner join {{ source('justicia_finpro_gold', 'dim_products') }} b
      on a.status = 'genuine'
        and a.product_pk=b.product_pk
  GROUP BY order_date_WIB, a.country,b.category
  order by order_date_WIB asc, amount desc


