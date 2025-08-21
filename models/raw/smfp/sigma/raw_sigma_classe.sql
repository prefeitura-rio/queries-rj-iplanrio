{{
    config(
        alias='classe',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_classe AS STRING) AS codigo_classe,
    SAFE_CAST(cd_grupo AS STRING) AS codigo_grupo,
    SAFE_CAST(ds_classe AS STRING) AS descricao_classe,
    SAFE_CAST(st_status AS STRING) AS status,
FROM {{ source('compras_materiais_servicos_sigma_staging', 'VW_CLASSE') }}