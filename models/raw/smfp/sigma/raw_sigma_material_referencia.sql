{{
    config(
        alias='material_referencia',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_classe AS STRING) AS codigo_classe,
    SAFE_CAST(cd_grupo AS STRING) AS codigo_grupo,
    SAFE_CAST(cd_material AS STRING) AS codigo_material,
    SAFE_CAST(cd_referencia AS STRING) AS codigo_referencia,
    SAFE_CAST(cd_subclasse AS STRING) AS codigo_subclasse,
    SAFE_CAST(ds_referencia AS STRING) AS descricao_referencia,
    SAFE_CAST(dv1 AS STRING) AS digito_verificador_1,
    SAFE_CAST(dv2 AS STRING) AS digito_verificador_2,
    SAFE_CAST(sequencial AS STRING) AS sequencial_material,
    SAFE_CAST(st_status AS STRING) AS status,

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_MATERIAL_REFERENCIA')}}