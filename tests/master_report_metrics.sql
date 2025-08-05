SELECT
    *
FROM
    {{ ref('master_report') }}
WHERE m1 > 0 and m2 > 0