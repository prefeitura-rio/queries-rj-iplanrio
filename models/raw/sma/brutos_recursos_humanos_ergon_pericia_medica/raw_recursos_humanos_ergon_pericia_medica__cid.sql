{{
    config(
        alias='cid',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "cid"],
        description="Classificação Internacional de Doenças (CID)."
    )
}}

SELECT
    SAFE_CAST(tipo AS STRING) AS tipo,
    SAFE_CAST(codigo AS STRING) AS codigo,
    SAFE_CAST(descricao AS STRING) AS descricao,
    SAFE_CAST(cod_pai AS STRING) AS codigo_pai,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    SAFE_CAST(flex_campo_11 AS STRING) AS flex_campo_11,
    SAFE_CAST(flex_campo_12 AS STRING) AS flex_campo_12,
    SAFE_CAST(flex_campo_13 AS STRING) AS flex_campo_13,
    SAFE_CAST(flex_campo_14 AS STRING) AS flex_campo_14,
    SAFE_CAST(flex_campo_15 AS STRING) AS flex_campo_15,
    SAFE_CAST(capitulo AS STRING) AS capitulo,
    SAFE_CAST(guid_grupo AS STRING) AS guid_grupo,
    SAFE_CAST(descricao_abrev AS STRING) AS descricao_abreviada,
    SAFE_CAST(classif AS STRING) AS classificacao,
    SAFE_CAST(refer AS STRING) AS referencia,
    SAFE_CAST(restr_sexo AS STRING) AS restricao_sexo,
    SAFE_CAST(causa_obito AS STRING) AS causa_obito,
    SAFE_CAST(excluidos AS STRING) AS excluidos,
    SAFE_CAST(nivel AS STRING) AS nivel,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'cid') }} AS t
