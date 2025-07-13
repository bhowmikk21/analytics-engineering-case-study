{{
  config(
    materialized = 'view',
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"E"', 'parent_referance']) }} as parent_id,
    'E' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"E"', 'child_referance']) }} as child_id,
    'E' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'e') }};