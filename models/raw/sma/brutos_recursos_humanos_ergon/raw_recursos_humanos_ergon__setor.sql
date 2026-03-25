{{
    config(
        alias='setor',
        materialized="table",
        tags=["raw", "ergon", "setor"],
        description="Tabela que contém os registros dos setores da administração direta ou indireta da prefeitura do Rio de Janeiro."
    )
}}

SELECT
    safe_cast(SETOR as string) as id_setor,
    safe_cast(paisetor as string) as id_setor_pai,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DATAINI) AS DATE) AS data_inicio,
    SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',DATAFIM) AS DATE) AS data_fim,
    SAFE_CAST(NOMESETOR AS STRING) AS nome,
    SAFE_CAST(NOMESETORLONGO AS STRING) AS nome_completo,
    SAFE_CAST(FLEX_CAMPO_01 AS STRING) AS sigla,
    safe_cast(emp_codigo as string) as id_empresa,
    safe_cast(FLEX_CAMPO_02 as string) as id_empresa_prevrio,
    safe_cast(FLEX_CAMPO_05 as string) as id_secretaria,
    SAFE_CAST(ifnull(EXTINTO, 'N') AS STRING) AS extinto,
    _airbyte_extracted_at AS updated_at
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_HSETOR_')}}