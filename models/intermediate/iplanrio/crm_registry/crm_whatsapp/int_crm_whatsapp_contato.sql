{{
  config(
    alias="int_crm_whatsapp_contato",
    schema="intermediario_crm_whatsapp", 
    materialized='incremental',
    tags=["hourly"],
    unique_key='id_contato',
    incremental_strategy='merge',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH
source AS (select * from {{ source("brutos_wetalkie_staging", "contato_faltante") }}),

missing_contacts AS (
  SELECT
  CAST(id_contato AS STRING) AS id_contato,
  contato_nome,
  contato_telefone
  FROM source
  where contato_telefone is not null
),

ura_contacts AS (
  SELECT 
    contato_nome,
    CAST(id_contato AS STRING) AS id_contato,
    data_update,
    data_optin
  FROM {{ ref("int_chatbot_base_receptivo_contatos") }}
  
  WHERE
  -- data que migrou para o ambiente de produção, se tirar teremos id_contato desconhecidos "2025-07-09"
  DATE(data_update) >= "2025-05-24"
  -- {% if is_incremental() %}
  --    AND DATE(data_update) >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 5 DAY)
  -- {% endif %}
),

hsm_contacts AS (
  SELECT
    CAST(id_contato AS STRING) AS id_contato,
    contato_telefone,
    MIN(data_particao) AS data_optin,
    MAX(data_particao) AS data_update,
    MAX(CASE
      WHEN descricao_falha LIKE "%131048%" THEN data_particao ELSE NULL END) AS data_inicio_quarentena
  FROM {{ ref("int_chatbot_base_disparo") }}

  WHERE
  -- data que migrou para o ambiente de produção, se tirar teremos id_contato desconhecidos "2025-07-09"
  DATE(data_particao) >= "2025-05-24" AND
  {% if is_incremental() %}
    DATE(data_particao) >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 5 DAY)
  {% else %}
    TRUE
  {% endif %}
  GROUP BY 1, 2
),

new_records AS (
  SELECT
    DISTINCT
    COALESCE(ura_contacts.id_contato, hsm_contacts.id_contato) AS id_contato,
    ura_contacts.contato_nome,
    hsm_contacts.contato_telefone,
    LEAST(
      IFNULL(ura_contacts.data_optin, CURRENT_DATE()), 
      IFNULL(hsm_contacts.data_optin, CURRENT_DATE())
    ) AS new_data_optin,
    hsm_contacts.data_inicio_quarentena,
    DATE_ADD(hsm_contacts.data_inicio_quarentena, INTERVAL 6 MONTH) AS data_fim_quarentena,
    "whatsapp" AS origem_update,
    GREATEST(
      IFNULL(ura_contacts.data_update, DATE("1990-01-01")), 
      IFNULL(hsm_contacts.data_update, DATE("1990-01-01"))
    ) AS data_update,
    GREATEST(
      IFNULL(ura_contacts.data_update, DATE("1990-01-01")), 
      IFNULL(hsm_contacts.data_update, DATE("1990-01-01"))
    ) AS data_particao
  FROM ura_contacts
  FULL OUTER JOIN hsm_contacts USING(id_contato)
),

final AS (
    SELECT
    n.id_contato,
    null AS cpf,
    REGEXP_REPLACE(COALESCE(n.contato_nome, miss.contato_nome), r'[^\w\sÀ-ÿ]', '') AS contato_nome,
    COALESCE(n.contato_telefone, miss.contato_telefone) AS contato_telefone,
    {% if is_incremental() %}
        LEAST(IFNULL(t.data_optin, CURRENT_DATE()), n.new_data_optin) AS data_optin,
    {% else %}
        n.new_data_optin AS data_optin,
    {% endif %}
    null AS data_optout,
    null AS razao_optout,
    n.data_inicio_quarentena,
    n.data_fim_quarentena,
    n.origem_update,
    n.data_update,
    n.data_particao
    FROM new_records n
    LEFT JOIN missing_contacts miss USING (id_contato)

    {% if is_incremental() %}
    LEFT JOIN {{ this }} t ON n.id_contato = t.id_contato
    {% endif %}
)

SELECT
  CAST(id_contato AS STRING) AS id_contato,
  CAST(cpf AS STRING) AS cpf,
  {{ proper_br("contato_nome") }} AS contato_nome,
  contato_telefone,
  data_optin,
  data_optout,
  razao_optout,
  data_inicio_quarentena,
  data_fim_quarentena,
  origem_update,
  data_update,
  data_particao
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_contato ORDER BY data_update DESC, data_particao DESC) AS rn
  FROM final
)
WHERE rn = 1