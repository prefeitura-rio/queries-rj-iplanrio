{{
    config(
        alias="cronograma_alteracao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(
            nr_processo AS STRING
        ) id_processo,
        SAFE_CAST(cd_etapa AS STRING) id_etapa,
        SAFE_CAST(tp_alteracao AS STRING) tipo_alteracao,
        SAFE_CAST(
            dt_publ_do AS DATETIME
        ) AS data_publicacao,
        SAFE_CAST(
            dt_validade AS DATETIME
        ) AS data_validade,
        SAFE_CAST(nr_prazo AS INT64) dias_prazo,
        SAFE_CAST(ds_observacao AS STRING) observacao,
FROM {{ source('brutos_siscob_staging', 'cronograma_alteracao') }} AS t
