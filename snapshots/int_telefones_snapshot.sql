{% snapshot int_telefones_snapshot %}

{{
    config(
      target_schema='snapshots',
      alias="int_telefone",
      unique_key='telefone_numero_completo',
      strategy='check',
      check_cols=['rmi_versao'],
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('int_telefones') }}

{% endsnapshot %}