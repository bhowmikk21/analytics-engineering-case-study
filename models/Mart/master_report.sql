with base as (
SELECT
    gp.parent_ref AS grand_parent_ref,
    gp.parent_sys AS grand_parent_sys,
    ch.parent_ref,
    ch.parent_sys,
    ch.child_ref,
    ch.child_sys
FROM company_hierarchy ch
LEFT JOIN company_hierarchy gp ON ch.parent_ref = gp.child_ref AND ch.parent_sys = gp.child_sys
)
, base1 as (
select b.grand_parent_ref, c1.company_name as grand_parent, c1.status as grand_parent_status
	, b.parent_ref, c2.company_name as parent, c2.status as parent_status
, b.child_ref
from base b
left join company_metrics c1 on b.grand_parent_ref = c1.id
left join company_metrics c2 on b.parent_ref = c2.id
left join company_metrics c3 on b.child_ref = c3.id
)
SELECT COALESCE(ch.grand_parent, ch.parent, cm.company_name) AS name,
	cm.system_id AS system,
	cm.ref,
	coalesce(ch.grand_parent_status, ch.parent_status, cm.status) as status
	,cm.m1, cm.m2, cm.m3, cm.m4, cm.m5
FROM company_metrics cm
LEFT JOIN base1 ch ON cm.id = ch.child_ref
ORDER BY name

