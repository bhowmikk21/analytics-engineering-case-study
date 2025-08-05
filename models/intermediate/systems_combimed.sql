{{ config(
    materialized = 'table',
    tags = ['intermediate', 'system']
    ) 
}}

SELECT * FROM {{ ref('stg_system_a') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_b') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_c') }};
