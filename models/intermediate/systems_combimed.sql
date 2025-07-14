{{ config(
    materialized = 'table',
    tags = ['intermediate', 'system']
    ) 
}}

SELECT * FROM {{ ref('stg_system_a') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_b') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_c') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_d') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_e') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_f') }}
UNION ALL
SELECT * FROM {{ ref('stg_system_g') }};
