{% snapshot snapshot_dim_patient %}

{{
    config(
        target_schema='public',
        unique_key='patient_key',
        strategy='check',
        check_cols=['race', 'gender', 'age'],
    )
}}

select * from {{ source('raw_dw', 'dim_patient') }}

{% endsnapshot %}