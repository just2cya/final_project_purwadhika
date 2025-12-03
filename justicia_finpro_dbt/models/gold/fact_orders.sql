{{
  config(
    materialized='incremental',
    unique_key='order_pk',
    incremental_strategy='merge',
    partition_by={
      "field": "created_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['user_sk', 'product_sk', 'country']
  )
}}

SELECT
    GENERATE_UUID() AS order_pk,
    order_id,
    user_pk,
    product_pk,
    quantity,
    amount,
    country,
    created_date
  FROM {{ source('justicia_finpro_raw', 'raw_orders') }} a 
  LEFT JOIN {{ ref('dim_users') }} b
    ON a.user_id = b.user_id
  LEFT JOIN {{ ref('dim_products') }} c
    ON a.product_id = c.product_id

{% if is_incremental() %}

WHERE a.created_date > (SELECT MAX(created_date) FROM {{ this }})

{% endif %}