{{
    config(
        alias='tipo_endereco',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_tipo_endereco AS INT64) AS id_tipo_endereco,
    SAFE_CAST(no_tipo_endereco AS STRING) AS no_tipo_endereco,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_tipo_endereco') }}
