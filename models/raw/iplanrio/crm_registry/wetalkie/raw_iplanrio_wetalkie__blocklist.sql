{{
  config(
    materialized='table',
    schema='brutos_wetalkie',
    alias='blocklist_temp'
  )
}}

SELECT
  CAST(phone AS STRING) as contato_telefone,
  profile_name as contato_nome,
  date_time as data_bloqueio,
  reason as razao_bloqueio,
  DATE(date_time) as data_particao
FROM `rj-crm-registry.brutos_wetalkie_staging.blocklist`
