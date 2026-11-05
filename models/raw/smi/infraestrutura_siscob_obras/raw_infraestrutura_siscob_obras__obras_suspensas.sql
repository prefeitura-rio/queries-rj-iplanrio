{{
    config(
        alias="obras_suspensas",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(ds_titulo_objeto AS STRING) titulo_objeto,
        SAFE_CAST(dt_suspensao AS DATE
        ) AS data_suspensao,
        SAFE_CAST(ds_motivo AS STRING) motivo_suspensao,
        SAFE_CAST(ds_previsao AS STRING) previsao_retomada,
        SAFE_CAST(ds_justificativa AS STRING) justificativa,
        SAFE_CAST(nm_responsavel AS STRING) nome_responsavel
FROM {{ source('brutos_siscob_staging', 'obras_suspensas') }} AS t