{{
    config(
        alias='fornecedor',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_fornecedor AS STRING)), r'\.0$', '') AS STRING) AS id_fornecedor,
  SAFE_CAST(TRIM(SAFE_CAST(cod_fornecedor AS STRING)) AS STRING) AS cod_fornecedor,
  SAFE_CAST(TRIM(SAFE_CAST(fornecedor AS STRING)) AS STRING) AS fornecedor,
  SAFE_CAST(TRIM(SAFE_CAST(tipo_pessoa AS STRING)) AS STRING) AS tipo_pessoa,
  SAFE_CAST(TRIM(SAFE_CAST(endereco AS STRING)) AS STRING) AS endereco,
  SAFE_CAST(TRIM(SAFE_CAST(numero AS STRING)) AS STRING) AS numero,
  SAFE_CAST(TRIM(SAFE_CAST(complemento AS STRING)) AS STRING) AS complemento,
  SAFE_CAST(TRIM(SAFE_CAST(cep AS STRING)) AS STRING) AS cep,
  SAFE_CAST(TRIM(SAFE_CAST(bairro AS STRING)) AS STRING) AS bairro,
  SAFE_CAST(TRIM(SAFE_CAST(municipio AS STRING)) AS STRING) AS municipio,
  SAFE_CAST(TRIM(SAFE_CAST(uf AS STRING)) AS STRING) AS uf,
  SAFE_CAST(TRIM(SAFE_CAST(referencia AS STRING)) AS STRING) AS referencia,
  SAFE_CAST(TRIM(SAFE_CAST(telefone_1 AS STRING)) AS STRING) AS telefone_1,
  SAFE_CAST(TRIM(SAFE_CAST(telefone_1_ramal AS STRING)) AS STRING) AS telefone_1_ramal,
  SAFE_CAST(TRIM(SAFE_CAST(telefone_2 AS STRING)) AS STRING) AS telefone_2,
  SAFE_CAST(TRIM(SAFE_CAST(telefone_2_ramal AS STRING)) AS STRING) AS telefone_2_ramal,
  SAFE_CAST(TRIM(SAFE_CAST(telefone_fax AS STRING)) AS STRING) AS telefone_fax,
  SAFE_CAST(TRIM(SAFE_CAST(email AS STRING)) AS STRING) AS email,
  SAFE_CAST(TRIM(SAFE_CAST(contato AS STRING)) AS STRING) AS contato,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_log_importacao AS STRING)), r'\.0$', '') AS STRING) AS id_log_importacao,
  SAFE_CAST(TRIM(SAFE_CAST(cod_organizacao AS STRING)) AS STRING) AS cod_organizacao,
  SAFE_CAST(data_envio_data_envio AS DATETIME) AS data_envio
FROM {{ source('brutos_osinfo_staging', 'fornecedor') }} AS t
