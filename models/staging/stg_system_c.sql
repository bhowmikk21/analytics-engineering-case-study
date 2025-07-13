{{
  config(
    materialized = 'view',
    tags = ['staging', 'system_c']
  )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"C"', 'ref']) }} as company_id,
    'C' AS system_name,
    name AS company_name,
    ref AS company_ref,
    status AS status_summary,
    CAST(m1 AS INTEGER) AS "M1",
    CAST(m2 AS FLOAT) AS "M2",
    CAST(m3 AS INTEGER) AS "M3",
    CAST(m4 AS INTEGER) AS "M4",
    CAST(m5 AS FLOAT) AS "M5",
    CURRENT_TIMESTAMP() AS report_date
FROM {{ source('systems', 'c') }};