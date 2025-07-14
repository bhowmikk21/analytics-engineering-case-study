{{ config(
    materialized = 'table',
    tags = ['mart', 'master_report']
    ) 
}}

SELECT
    COALESCE(s1.company_name, s.company_name) AS name,
    s.system_id AS system,
    s.ref,
    COALESCE(s1.status, s.status) AS status,
    s.m1, s.m2, s.m3, s.m4, s.m5
FROM systems_combined s
LEFT JOIN heirarchy_combined h
  ON c.id = h.child_ref
LEFT JOIN systems_combined s1
  ON h.parent_ref = s1.id;