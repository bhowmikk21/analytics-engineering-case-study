{{
  config(
    materialized = 'view',
    tags = ['staging', 'heirarchy']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['parent_system', 'parent_referance']) }} as parent_id,
    parent_system,
    parent_referance,
    {{ dbt_utils.generate_surrogate_key(['child_system', 'child_referance']) }} as child_id,
    child_system,
    child_referance
FROM {{ source('heirarchy', 'multi_source') }};