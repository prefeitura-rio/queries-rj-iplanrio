{{
    config(
        alias="grupo",
        description="Grupo da Lista de Classificação"
    )
}}

SELECT
    SAFE_CAST(CD_GRUPO AS STRING) AS codigo_grupo,
    SAFE_CAST(DS_GRUPO AS STRING) AS descricao_grupo,
    SAFE_CAST(ST_STATUS AS NUMERIC) AS status
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'grupo') }}