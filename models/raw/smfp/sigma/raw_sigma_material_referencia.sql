{{
    config(
        alias='material_referencia',
        description="Referência de Material"
    )
}}

SELECT
    SAFE_CAST(CD_MATERIAL AS STRING) AS codigo_material,
    SAFE_CAST(CD_GRUPO AS STRING) AS codigo_grupo,
    SAFE_CAST(CD_CLASSE AS STRING) AS codigo_classe,
    SAFE_CAST(CD_SUBCLASSE AS STRING) AS codigo_subclasse,
    SAFE_CAST(SEQUENCIAL AS STRING) AS sequencial_material,
    SAFE_CAST(DV1 AS STRING) AS digito_verificador_1,
    SAFE_CAST(DV2 AS STRING) AS digito_verificador_2,
    SAFE_CAST(CD_REFERENCIA AS NUMERIC) AS codigo_referencia,
    SAFE_CAST(DS_REFERENCIA AS STRING) AS descricao_referencia,
    SAFE_CAST(ST_STATUS AS STRING) AS status
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'material_referencia') }}