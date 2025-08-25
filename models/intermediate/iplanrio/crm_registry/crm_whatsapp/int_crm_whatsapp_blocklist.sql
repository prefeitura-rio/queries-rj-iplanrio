{{ config(
    materialized = 'table',
    alias="blocklist",
    schema="brutos_wetalkie",
    tags=["hourly"],
    partition_by={
        "field": "data_particao",
        "data_type": "date"
    }
) }}

SELECT
  CAST(phone AS STRING) as contato_telefone,
  profile_name as contato_nome,
  date_time as data_bloqueio,
  reason as razao_bloqueio,
  DATE(date_time) as data_particao
-- TODO: mudar a origem do dado
FROM `rj-crm-registry-dev.patricia___brutos_wetalkie.blocklist`
WHERE phone IS NOT NULL AND date_time IS NOT NULL
