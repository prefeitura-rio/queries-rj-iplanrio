
{{
    config(
        alias='registros',
        schema='protecao_social_cadunico',
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0601'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0603'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0604'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_21,

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0609'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_21,

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0612'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0615'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0617'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0619'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0620'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0623'
    AND SUBSTRING(text,38,2) = '99'

UNION ALL


SELECT

    --column: num_reg_arquivo
    CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,38,2), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,38,2))
        END AS STRING
    ) AS numero_registro_arquivo,

    --column: qtd_reg_00_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,40,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,40,9))
        END AS INT64
    ) AS registros_tipo_00,

    --column: qtd_reg_01_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,49,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,49,9))
        END AS INT64
    ) AS registros_tipo_01,

    --column: qtd_reg_02_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,58,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,58,9))
        END AS INT64
    ) AS registros_tipo_02,

    --column: qtd_reg_03_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,67,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,67,9))
        END AS INT64
    ) AS registros_tipo_03,

    --column: qtd_reg_04_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,76,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,76,9))
        END AS INT64
    ) AS registros_tipo_04,

    --column: qtd_reg_05_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,85,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,85,9))
        END AS INT64
    ) AS registros_tipo_05,

    --column: qtd_reg_06_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,94,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,94,9))
        END AS INT64
    ) AS registros_tipo_06,

    --column: qtd_reg_07_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,103,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,103,9))
        END AS INT64
    ) AS registros_tipo_07,

    --column: qtd_reg_08_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,112,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,112,9))
        END AS INT64
    ) AS registros_tipo_08,

    --column: qtd_reg_09_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,121,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,121,9))
        END AS INT64
    ) AS registros_tipo_09,

    --column: qtd_reg_10_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,130,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,130,9))
        END AS INT64
    ) AS registros_tipo_10,

    --column: qtd_reg_11_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,139,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,139,9))
        END AS INT64
    ) AS registros_tipo_11,

    --column: qtd_reg_12_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,148,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,148,9))
        END AS INT64
    ) AS registros_tipo_12,

    --column: qtd_reg_13_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,157,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,157,9))
        END AS INT64
    ) AS registros_tipo_13,

    --column: qtd_reg_14_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,166,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,166,9))
        END AS INT64
    ) AS registros_tipo_14,

    --column: qtd_reg_15_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,175,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,175,9))
        END AS INT64
    ) AS registros_tipo_15,

    --column: qtd_reg_16_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,184,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,184,9))
        END AS INT64
    ) AS registros_tipo_16,

    --column: qtd_reg_17_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,193,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,193,9))
        END AS INT64
    ) AS registros_tipo_17,

    --column: qtd_reg_18_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,202,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,202,9))
        END AS INT64
    ) AS registros_tipo_18,

    --column: qtd_reg_19_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,211,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,211,9))
        END AS INT64
    ) AS registros_tipo_19,

    --column: qtd_reg_20_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,220,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,220,9))
        END AS INT64
    ) AS registros_tipo_20,

    --column: qtd_reg_21_tlr
    NULL AS registros_tipo_21, --Essa coluna não esta na versao posterior

    --column: qtd_reg_98_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,229,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,229,9))
        END AS INT64
    ) AS registros_tipo_98,

    --column: qtd_reg_99_tlr
    SAFE_CAST(
        CASE
            WHEN REGEXP_CONTAINS(SUBSTRING(text,238,9), r'^\s*$') THEN NULL
            ELSE TRIM(SUBSTRING(text,238,9))
        END AS INT64
    ) AS registros_tipo_99,
    SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
    SAFE_CAST(data_particao AS DATE) AS data_particao
FROM `rj-iplanrio.brutos_cadunico_staging.registro_familia`
WHERE versao_layout_particao = '0624'
    AND SUBSTRING(text,38,2) = '99'

