{{
  config(
    materialized = 'view',
    tags = ['staging', 'heirarchy', 'system_b']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"B"', 'parent_referance']) }} as parent_id,
    'B' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"B"', 'child_referance']) }} as child_id,
    'B' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'b') }};