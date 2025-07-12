{{
  config(
    materialized = 'view',
    description = 'Standardizes and stages customer name mapping for downstream enrichment.'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['"A"', 'ref']) }} as company_id,
    'A' AS system_name,
    name AS company_name_raw,
    ref AS company_ref,
    status AS status_summary,
    CAST(m1 AS INTEGER) AS metric_1,
    CAST(m2 AS FLOAT) AS metric_2,
    CAST(m3 AS INTEGER) AS metric_3,
    CAST(m4 AS INTEGER) AS metric_4,
    CAST(m5 AS FLOAT) AS metric_5,
    CURRENT_DATE AS report_date
FROM {{ source('system_a', 'report') }};