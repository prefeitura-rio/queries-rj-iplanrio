{{
    config(
        alias='ramo_atividade',
        description="Cadastro do Ramo de Atividades"
    )
}}

SELECT
    SAFE_CAST(CD_RAMO AS NUMERIC) AS codigo_ramo,
    SAFE_CAST(DS_RAMO AS STRING) AS descricao_ramo,
    SAFE_CAST(ST_RAMO AS STRING) AS situacao_ramo
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'ramo_atividade') }}