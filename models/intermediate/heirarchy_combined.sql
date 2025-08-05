{{ config(
    materialized = 'table',
    tags = ['intermediate', 'hierarchy']
    ) 
}}

SELECT * FROM {{ ref('stg_ind_heirarchy_a') }}
UNION ALL
SELECT * FROM {{ ref('stg_ind_heirarchy_b') }}
UNION ALL
SELECT * FROM {{ ref('stg_ind_heirarchy_c') }}
UNION ALL
SELECT * FROM {{ ref('stg_multi_source_heirarchy') }};
