{{ config(
    materialized = 'table',
    tags = ['mart', 'final']
    ) 
}}

with hierarchy_flaten as (
SELECT
    gp.parent_ref AS grand_parent_ref,
    gp.parent_sys AS grand_parent_sys,
    ch.parent_ref,
    ch.parent_sys,
    ch.child_ref,
    ch.child_sys
FROM heirarchy_combined ch
LEFT JOIN heirarchy_combined gp ON ch.parent_ref = gp.child_ref AND ch.parent_sys = gp.child_sys
)
, systems_heirarchy as (
select h.grand_parent_ref, sc1.company_name as grand_parent, sc1.status as grand_parent_status
  , h.parent_ref, sc2.company_name as parent, sc2.status as parent_status
  , h.child_ref
from hierarchy_flaten h
left join systems_combines sc1 on h.grand_parent_ref = sc1.id
left join systems_combines sc2 on h.parent_ref = sc2.id
left join systems_combines sc3 on h.child_ref = sc3.id
)
SELECT COALESCE(sh.grand_parent, sh.parent, sc.company_name) AS name,
	sc.system_id AS system,
	sc.ref,
	coalesce(sh.grand_parent_status, sh.parent_status, sc.status) as status
	,sc.m1, sc.m2, sc.m3, sc.m4, sc.m5
FROM systems_combined sc
LEFT JOIN systems_heirarchy sh ON sc.id = sh.child_ref
ORDER BY name

