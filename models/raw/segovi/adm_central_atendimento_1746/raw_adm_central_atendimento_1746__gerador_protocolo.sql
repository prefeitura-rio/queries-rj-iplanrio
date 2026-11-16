{{
    config(
        alias='gerador_protocolo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(cd_chave_unica_cti AS STRING) AS cd_chave_unica_cti,
    SAFE_CAST(ds_codigo AS STRING) AS ds_codigo,
    SAFE_CAST(ic_origem AS STRING) AS ic_origem,
    SAFE_CAST(id_gerador_protocolo AS INT64) AS id_gerador_protocolo,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_gerador_protocolo') }}
