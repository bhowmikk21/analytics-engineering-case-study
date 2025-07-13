{{
  config(
    materialized = 'view',
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"D"', 'parent_referance']) }} as parent_id,
    'D' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"D"', 'child_referance']) }} as child_id,
    'D' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'd') }};