{{
    config(
        alias='reclassificacao',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_justificativa AS STRING) AS ds_justificativa,
    SAFE_CAST(dt_reclassificacao AS DATETIME) AS dt_reclassificacao,
    SAFE_CAST(ic_origem AS STRING) AS ic_origem,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_classificacao_anterior_fk AS INT64) AS id_classificacao_anterior_fk,
    SAFE_CAST(id_classificacao_nova_fk AS INT64) AS id_classificacao_nova_fk,
    SAFE_CAST(id_pessoa_operador_fk AS INT64) AS id_pessoa_operador_fk,
    SAFE_CAST(id_reclassificacao AS INT64) AS id_reclassificacao,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_reclassificacao') }}
