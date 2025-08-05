{% snapshot scd_raw_listings %}

{{
   config(
       target_schema='DEV',
       unique_key='parent_ref',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select * FROM {{ source('heirarchy', 'multi_source') }}

{% endsnapshot %}