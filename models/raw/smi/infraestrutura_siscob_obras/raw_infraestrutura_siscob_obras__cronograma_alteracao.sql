{{
    config(
        alias="cronograma_alteracao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING) id_obra,
        SAFE_CAST(
            REGEXP_REPLACE(nr_processo, r'\.0$', '') AS STRING
        ) id_processo,
        SAFE_CAST(REGEXP_REPLACE(cd_etapa, r'\.0$', '') AS STRING) id_etapa,
        SAFE_CAST(tp_alteracao AS STRING) tipo_alteracao,
        SAFE_CAST(
            SAFE.PARSE_TIMESTAMP ('%Y-%m-%d %H:%M:%S', dt_publ_do) AS DATETIME
        ) AS data_publicacao,
        SAFE_CAST(
            SAFE.PARSE_TIMESTAMP ('%Y-%m-%d %H:%M:%S', dt_validade) AS DATETIME
        ) AS data_validade,
        SAFE_CAST(nr_prazo AS INT64) dias_prazo,
        SAFE_CAST(ds_observacao AS STRING) observacao,
FROM {{ source('brutos_siscob_staging', 'cronograma_alteracao') }} AS t
