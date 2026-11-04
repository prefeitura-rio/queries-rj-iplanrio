{{
    config(
        alias="programa_fonte",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(cd_obra AS STRING) id_obra,
        SAFE_CAST(
            cd_prg_trab AS STRING
        ) id_programa_trabalho,
        SAFE_CAST(programa_trabalho AS STRING) programa_trabalho,
        SAFE_CAST(
            cd_fonte_recurso AS STRING
        ) id_fonte_recurso,
        SAFE_CAST(fonte_recurso AS STRING) fonte_recurso,
        SAFE_CAST(
            cd_natureza_dsp AS STRING
        ) id_natureza_despesa,
        SAFE_CAST(natureza_despesa AS STRING) natureza_despesa,
FROM {{ source('brutos_siscob_staging', 'programa_fonte') }} AS t