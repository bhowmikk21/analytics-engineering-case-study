{{
  config(
    materialized = 'view',
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"F"', 'parent_referance']) }} as parent_id,
    'F' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"F"', 'child_referance']) }} as child_id,
    'F' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'f') }};