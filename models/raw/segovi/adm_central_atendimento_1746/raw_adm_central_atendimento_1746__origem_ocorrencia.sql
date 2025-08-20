{{
    config(
        alias='origem_ocorrencia',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_origem_ocorrencia',
    )
}}

SELECT
    SAFE_CAST(REGEXP_REPLACE(TRIM(id_origem_ocorrencia), r'\.0$', '') AS STRING) AS id_origem_ocorrencia,
    SAFE_CAST(TRIM(no_origem_ocorrencia) AS STRING) AS no_origem_ocorrencia
FROM {{ source('brutos_1746_staging', 'origem_ocorrencia') }}