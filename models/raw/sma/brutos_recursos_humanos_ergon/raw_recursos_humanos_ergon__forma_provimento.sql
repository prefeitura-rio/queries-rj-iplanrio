{{
    config(
        alias='forma_provimento',
    )
}}

SELECT
    SAFE_CAST(sigla AS int64) AS id_forma_provimento,
    SAFE_CAST(nome AS STRING) AS nome,
    SAFE_CAST(inativo AS STRING) AS inativo,
    SAFE_CAST(primeiro_prov AS STRING) AS primeiro_provimento,
    SAFE_CAST(ativo AS STRING) AS ativo
FROM {{ source('brutos_ergon_staging', 'VW_DLK_ERG_FORMAS_PROV_') }} AS t