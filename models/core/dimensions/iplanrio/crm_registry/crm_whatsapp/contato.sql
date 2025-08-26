{{
  config(
    alias="contato",
    schema="crm_whatsapp", 
    materialized='incremental',
    tags=["quarter_hourly"],
    unique_key='id_contato',
    incremental_strategy='merge',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

select * from {{ ref('int_crm_whatsapp_contato') }}
{% if is_incremental() %}
    where data_particao > (select max(data_particao) from {{ this }})
{% endif %}
