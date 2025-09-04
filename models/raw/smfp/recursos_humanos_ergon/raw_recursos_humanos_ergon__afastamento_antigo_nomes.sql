{{
    config(
        alias='afastamento_antigo_nomes',
    )
}}

SELECT
    SAFE_CAST(EMP_CODIGO AS STRING) AS id_empresa,
    SAFE_CAST(AFAST_COD AS STRING) AS id_afastamento,
    SAFE_CAST(AFAST_DESCR AS STRING) AS nome_afastamento,
FROM {{ source('brutos_ergon_staging', 'SIRHU_DBTABELAS_AFASTAMENTO') }} AS t