{{
    config(
        alias='setor',
        materialized="table",
        tags=["raw", "ergon", "setor"],
        description="Tabela que contém os registros dos setores da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    safe_cast (SETOR AS string) AS id_setor,
    safe_cast (paisetor AS string) AS id_setor_pai,
    DATETIME(DATAINI) AS data_inicio,
    DATETIME(DATAFIM) AS data_fim,
    SAFE_CAST (NOMESETOR AS STRING) AS nome,
    SAFE_CAST (NOMESETORLONGO AS STRING) AS nome_completo,
    SAFE_CAST (FLEX_CAMPO_01 AS STRING) AS sigla,
    safe_cast (emp_codigo AS string) AS id_empresa,
    safe_cast (FLEX_CAMPO_02 AS string) AS id_empresa_prevrio,
    safe_cast (FLEX_CAMPO_05 AS string) AS id_secretaria,
    SAFE_CAST (ifnull (EXTINTO, 'N') AS STRING) AS extinto,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'vw_dlk_erg_hsetor_')}}