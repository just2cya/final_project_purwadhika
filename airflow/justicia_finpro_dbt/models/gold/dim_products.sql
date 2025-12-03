{{
  config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
  )
}}

SELECT
    GENERATE_UUID() AS product_pk,
    product_id,
    product_name,
    category,
    price,
    created_date
FROM {{ source('justicia_finpro_raw', 'raw_products') }}

{% if is_incremental() %}

WHERE created_date >= (SELECT MAX(created_date) FROM {{ this }})

{% endif %}