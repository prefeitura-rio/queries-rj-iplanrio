{{
    config(
        alias='afastamento_antigo_nomes',
        materialized="table",
        tags=["raw", "ergon", "afastamento_antigo"],
        description="Tabela que armazena informações sobre os códigos de afastamentos utilizados."
    )
}}

SELECT
    SAFE_CAST(EMP_CODIGO AS int64) AS id_empresa,
    SAFE_CAST(AFAST_COD AS int64) AS id_afastamento,
    SAFE_CAST(AFAST_DESCR AS string) AS nome_afastamento
FROM {{ source('brutos_ergon_staging', 'SIRHU_DBTABELAS_AFASTAMENTO') }} AS t