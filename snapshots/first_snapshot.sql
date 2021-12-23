{% snapshot orders_snapshot %}

{{
    config(
      target_database='BIKESTORES',
      target_schema='snapshots',
      unique_key='brand_id',

      strategy='timestamp',
      updated_at='brand_id'
    )
}}

select * from {{ source('PRODUCTION', 'BRANDS') }}

{% endsnapshot %}