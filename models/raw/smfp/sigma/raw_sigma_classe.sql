{{
    config(
        alias='classe',
        description='Classes de Material da Lista de Classificação'
    )
}}

SELECT
    SAFE_CAST(CD_GRUPO AS STRING) AS codigo_grupo,
    SAFE_CAST(CD_CLASSE AS STRING) AS codigo_classe,
    SAFE_CAST(DS_CLASSE AS STRING) AS descricao_classe,
    SAFE_CAST(ST_STATUS AS NUMERIC) AS status
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'classe') }}