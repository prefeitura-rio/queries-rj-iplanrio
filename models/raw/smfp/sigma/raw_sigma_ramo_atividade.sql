{{
    config(
        alias='ramo_atividade',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_ramo AS STRING) AS codigo_ramo,
    SAFE_CAST(ds_ramo AS STRING) AS descricao_ramo,
    SAFE_CAST(st_ramo AS STRING) AS situacao_ramo,

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_RAMO_ATIVIDADE')}}