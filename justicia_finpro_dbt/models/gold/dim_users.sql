SELECT
    GENERATE_UUID() AS user_pk,
    user_id,
    name,
    email,
    created_date
FROM {{ source('justicia_finpro_raw', 'raw_users') }};