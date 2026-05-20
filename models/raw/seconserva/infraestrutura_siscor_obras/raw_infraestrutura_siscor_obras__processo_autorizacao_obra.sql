{{
    config(
        alias='processo_autorizacao_obra',
        schema='infraestrutura_siscor_obras',
        materialized='table',
    )

}}

SELECT
    DISTINCT
    SAFE_CAST(Secretaria AS STRING) AS prefixo_numero_processo,
    SAFE_CAST(processo AS STRING) AS numero_processo,
    SAFE_CAST(sqnc_processo AS INT64) AS sequencial_processo,
    SAFE_CAST(dscr_tp_processo AS STRING) AS tipo_processo,
    SAFE_CAST(dt_protocolo AS DATETIME) AS data_abertura_protocolo,
    SAFE_CAST(dt_inicio_obra AS DATETIME) AS data_inicio_obra,
    SAFE_CAST(dt_fim_obra AS DATETIME) AS data_fim_obra,
    SAFE_CAST(area_ocupacao AS FLOAT64) AS area_ocupacao_obra,
    SAFE_CAST(observacao AS STRING) AS observacao,
    SAFE_CAST(prz_execucao AS INT64) AS prazo_execucao_obra,
    SAFE_CAST(dt_aprovacao AS DATETIME) AS data_aprovacao_obra,
    SAFE_CAST(natureza_obra AS STRING) AS natureza_obra,
    SAFE_CAST(tp_obra AS STRING) AS tipo_obra,
    SAFE_CAST(lcl_obra AS STRING) AS local_obra,
    SAFE_CAST(cgc_requerente AS STRING) AS cgc_requerente,
    SAFE_CAST(requerente AS STRING) AS nome_requerente,
    -- SAFE_CAST(REGEXP_REPLACE(LTRIM(cdg_logradouro ,'0'), r'\.0$', '') AS STRING) id_logradouro,
    SAFE_CAST(cdg_logradouro AS STRING) codigo_logradouro,
    SAFE_CAST(nm_lgrdr AS STRING) AS nome_completo_logradouro,
    SAFE_CAST(dt_plenario AS DATETIME) AS data_plenario,
    SAFE_CAST(dscr_situacao AS STRING) AS situacao_obra,
    SAFE_CAST(dscr_parecer AS STRING) AS parecer_plenario,
    SAFE_CAST(nmr_licenca AS STRING) AS numero_licenca,
    SAFE_CAST(cgc_executor AS STRING) AS cgc_executor,
    SAFE_CAST(executor AS STRING) AS nome_executor
FROM {{ source('infraestrutura_siscor_obras_staging', 'processo_autorizacao_obra') }}