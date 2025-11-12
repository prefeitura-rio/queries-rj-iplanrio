{{
    config(
        alias='origem_ocorrencia',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_origem_ocorrencia',
    )
}}

WITH source_data AS (
  SELECT 
    _airbyte_extracted_at,
    CAST(fl_ativo AS STRING) AS fl_ativo,
    CAST(id_origem_ocorrencia AS STRING) AS id_origem_ocorrencia,
    CAST(no_origem_ocorrencia AS STRING) AS no_origem_ocorrencia,
    CAST(id_origem_ocorrencia_categoria_fk AS STRING) AS id_origem_ocorrencia_categoria_fk,
  FROM {{ source('brutos_1746_staging_airbyte', 'tb_origem_ocorrencia') }}
)

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_origem_ocorrencia), r'\.0$', '') AS STRING) AS id_origem_ocorrencia,
    SAFE_CAST(TRIM(no_origem_ocorrencia) AS STRING) AS no_origem_ocorrencia,
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_origem_ocorrencia_categoria_fk), r'\.0$', '') AS STRING) AS id_origem_ocorrencia_categoria_fk,
    SAFE_CAST(TRIM(fl_ativo) AS STRING) AS fl_ativo,
    SAFE_CAST(_airbyte_extracted_at AS DATETIME) AS extracted_at
FROM source_data AS t