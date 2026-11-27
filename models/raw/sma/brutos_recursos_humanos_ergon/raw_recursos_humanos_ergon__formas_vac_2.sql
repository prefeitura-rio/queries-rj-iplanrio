{{
    config(
        alias='formas_vac_2',
        materialized="table",
        tags=["raw", "ergon", "vacancia"],
        description="Formas de vacância existentes na administração (tabela formas_vac_)."
    )
}}

SELECT
    SAFE_CAST(sigla AS STRING) AS id_forma_vacancia,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(pontlei AS INT64) AS pontuacao_lei,
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
    SAFE_CAST(flex_campo_16 AS STRING) AS flex_campo_16,
    SAFE_CAST(flex_campo_17 AS STRING) AS flex_campo_17,
    SAFE_CAST(flex_campo_18 AS STRING) AS flex_campo_18,
    SAFE_CAST(flex_campo_19 AS STRING) AS flex_campo_19,
    SAFE_CAST(flex_campo_20 AS STRING) AS flex_campo_20,
    SAFE_CAST(codigo_caged AS STRING) AS codigo_caged,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'formas_vac_') }} AS t
