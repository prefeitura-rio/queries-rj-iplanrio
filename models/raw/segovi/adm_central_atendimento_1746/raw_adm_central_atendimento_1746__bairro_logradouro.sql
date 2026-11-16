{{
    config(
        alias='bairro_logradouro',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_bairro_fk AS INT64) AS id_bairro_fk,
    SAFE_CAST(id_bairro_logradouro AS INT64) AS id_bairro_logradouro,
    SAFE_CAST(id_logradouro_fk AS INT64) AS id_logradouro_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_bairro_logradouro') }}
