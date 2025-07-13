{{
  config(
    materialized = 'view',
    tags = ['staging', 'heirarchy', 'system_c']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"C"', 'parent_referance']) }} as parent_id,
    'C' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"C"', 'child_referance']) }} as child_id,
    'C' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'c') }};