{{
    config(
        alias='funcoes_ev_',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "funcoes_ev_"],
        description="Eventos de função dos funcionários."
    )
}}

SELECT
    SAFE_CAST(funcao AS STRING) AS id_funcao,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(controle_vaga AS STRING) AS tipo_controle_vaga,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
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
    SAFE_CAST(flex_campo_21 AS STRING) AS flex_campo_21,
    SAFE_CAST(flex_campo_22 AS STRING) AS flex_campo_22,
    SAFE_CAST(flex_campo_23 AS STRING) AS flex_campo_23,
    SAFE_CAST(flex_campo_24 AS STRING) AS flex_campo_24,
    SAFE_CAST(flex_campo_25 AS STRING) AS flex_campo_25,
    SAFE_CAST(flex_campo_26 AS STRING) AS flex_campo_26,
    SAFE_CAST(flex_campo_27 AS STRING) AS flex_campo_27,
    SAFE_CAST(flex_campo_28 AS STRING) AS flex_campo_28,
    SAFE_CAST(flex_campo_29 AS STRING) AS flex_campo_29,
    SAFE_CAST(flex_campo_30 AS STRING) AS flex_campo_30,
    SAFE_CAST(dt_inicio_contr_vaga AS DATE) AS data_inicio_controle_vaga,
    SAFE_CAST(id_reg AS STRING) AS id_registro,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'funcoes_ev_') }} AS t
