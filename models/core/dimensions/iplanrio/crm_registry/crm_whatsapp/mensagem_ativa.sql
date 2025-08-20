{{ config(alias="mensagem_ativa", schema="crm_whatsapp", materialized="table") }}

SELECT * FROM {{ ref('raw_iplanrio_wetalkie__mensagem_ativa') }}
ORDER BY CAST(id_hsm AS INT64)