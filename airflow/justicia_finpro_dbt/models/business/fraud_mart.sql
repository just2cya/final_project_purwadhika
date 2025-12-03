{{
  config(
    materialized='table'
  )
}}

SELECT
    a.country,
    count(a.order_id) AS order_count,
    sum(a.quantity) AS quantity,
    sum(a.amount) AS amount
  FROM {{ source('justicia_finpro_gold', 'fact_orders') }} a 
  WHERE a.status = 'fraud'
  GROUP BY a.country
