{{
    config(
        alias='folhas_emp_dlk',
        materialized="table",
        tags=["raw", "ergon", "folha", "folhas_emp"],
        description="Tabela que armazena as folhas de pagamento por empresa (view DLK)."
    )
}}

SELECT
    SAFE_CAST(mes_ano AS DATE) AS mes_ano_folha,
    SAFE_CAST(numero AS STRING) AS numero_folha,
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(tipo_folha AS STRING) AS tipo_folha,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(data_aber AS DATE) AS data_abertura,
    SAFE_CAST(mes_principal AS INT64) AS mes_principal,
    SAFE_CAST(meses_retro AS INT64) AS meses_retro,
    SAFE_CAST(meses_retro_full AS INT64) AS meses_retro_full,
    SAFE_CAST(meses_adiant AS INT64) AS meses_adiant,
    SAFE_CAST(meses_pre_ergon AS INT64) AS meses_pre_ergon,
    SAFE_CAST(data_proc AS DATETIME) AS data_processamento,
    SAFE_CAST(data_consol AS DATETIME) AS data_consolidacao,
    SAFE_CAST(responsavel AS STRING) AS responsavel,
    SAFE_CAST(tipo_calculo AS STRING) AS tipo_calculo,
    SAFE_CAST(data_credito AS DATE) AS data_credito,
    SAFE_CAST(destino AS STRING) AS destino,
    SAFE_CAST(expiracao AS INT64) AS expiracao,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(numero_sec AS STRING) AS numero_sec,
    SAFE_CAST(grupo_empresas AS STRING) AS grupo_empresas,
    SAFE_CAST(classe_calculo AS STRING) AS classe_calculo,
    SAFE_CAST(classe_estorno AS STRING) AS classe_estorno,
    SAFE_CAST(data_limites AS DATE) AS data_limites,
    SAFE_CAST(id_grupo_fol_seg AS STRING) AS id_grupo_fol_seg,
    SAFE_CAST(data_geraeleitos AS DATETIME) AS data_geraeleitos,
    SAFE_CAST(data_geraeleitos_ant AS DATETIME) AS data_geraeleitos_ant,
    SAFE_CAST(numero_terc AS STRING) AS numero_terc,
    SAFE_CAST(id_reg AS STRING) AS id_registro,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_folhas_emp') }} AS t
