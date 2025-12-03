{{
  config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
  )
}}

SELECT
    GENERATE_UUID() AS user_pk,
    user_id,
    name,
    email,
    created_date
FROM {{ source('justicia_finpro_raw', 'raw_users') }}

{% if is_incremental() %}

WHERE created_date >= (SELECT MAX(created_date) FROM {{ this }})

{% endif %}