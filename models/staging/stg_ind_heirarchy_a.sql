{{
  config(
    materialized = 'view',
    tags = ['staging', 'heirarchy', 'system_a']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"A"', 'parent_referance']) }} as parent_id
    , 'A' AS parent_system
    , parent_referance
    , {{ dbt_utils.generate_surrogate_key(['"A"', 'child_referance']) }} as child_id
    , 'A' AS child_system
    , child_referance
FROM {{ source('heirarchy', 'a') }};