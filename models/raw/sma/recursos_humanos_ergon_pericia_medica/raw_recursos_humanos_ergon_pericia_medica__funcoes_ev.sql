{{
    config(
        alias='funcoes_ev',
       
    )
}}

SELECT safe_cast(NOME AS STRING) AS nome, 
safe_cast(FUNCAO AS STRING) AS funcao,
safe_cast(ID_REG AS STRING) AS id_reg,
safe_cast(PONTPUBL AS STRING) AS ponto_publicado,
safe_cast(CONTROLE_VAGA AS STRING) AS controle_vaga,
safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01,
safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02,
safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03,
safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
safe_cast(FLEX_CAMPO_06 AS STRING) AS flex_campo_06,
safe_cast(FLEX_CAMPO_07 AS STRING) AS flex_campo_07,
safe_cast(FLEX_CAMPO_08 AS STRING) AS flex_campo_08,
safe_cast(FLEX_CAMPO_09 AS STRING) AS flex_campo_09,
safe_cast(FLEX_CAMPO_10 AS STRING) AS flex_campo_10,
safe_cast(FLEX_CAMPO_11 AS STRING) AS flex_campo_11,
safe_cast(FLEX_CAMPO_12 AS STRING) AS flex_campo_12,
safe_cast(FLEX_CAMPO_13 AS STRING) AS flex_campo_13,
safe_cast(FLEX_CAMPO_14 AS STRING) AS flex_campo_14,
safe_cast(FLEX_CAMPO_15 AS STRING) AS flex_campo_15,
safe_cast(FLEX_CAMPO_16 AS STRING) AS flex_campo_16,
safe_cast(FLEX_CAMPO_17 AS STRING) AS flex_campo_17,
safe_cast(FLEX_CAMPO_18 AS STRING) AS flex_campo_18,
safe_cast(FLEX_CAMPO_19 AS STRING) AS flex_campo_19,
safe_cast(FLEX_CAMPO_20 AS STRING) AS flex_campo_20,
safe_cast(FLEX_CAMPO_21 AS STRING) AS flex_campo_21,
safe_cast(FLEX_CAMPO_22 AS STRING) AS flex_campo_22,
safe_cast(FLEX_CAMPO_23 AS STRING) AS flex_campo_23,
safe_cast(FLEX_CAMPO_24 AS STRING) AS flex_campo_24,
safe_cast(FLEX_CAMPO_25 AS STRING) AS flex_campo_25,
safe_cast(FLEX_CAMPO_26 AS STRING) AS flex_campo_26,
safe_cast(FLEX_CAMPO_27 AS STRING) AS flex_campo_27,
safe_cast(FLEX_CAMPO_28 AS STRING) AS flex_campo_28,
safe_cast(FLEX_CAMPO_29 AS STRING) AS flex_campo_29,
safe_cast(FLEX_CAMPO_30 AS STRING) AS flex_campo_30,
safe_cast(DT_INICIO_CONTR_VAGA AS DATE) AS dt_inicio_contr_vaga 
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'FUNCOES_EV_') }}

