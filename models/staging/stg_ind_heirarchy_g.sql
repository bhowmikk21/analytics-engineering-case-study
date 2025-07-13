{{
  config(
    materialized = 'view',
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"G"', 'parent_referance']) }} as parent_id,
    'G' AS parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['"G"', 'child_referance']) }} as child_id,
    'G' AS child_system,
    child_referance
FROM {{ source('heirarchy', 'g') }};