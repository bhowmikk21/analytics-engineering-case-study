SELECT
    u.*,
    m.canonical_company_id,
    m.mapped_name AS canonical_company_name
FROM {{ ref('stg_all_sources_union') }} u
LEFT JOIN {{ ref('dim_company_mapping') }} m
  ON u.system_name = m.system_name AND u.company_ref = m.company_ref;