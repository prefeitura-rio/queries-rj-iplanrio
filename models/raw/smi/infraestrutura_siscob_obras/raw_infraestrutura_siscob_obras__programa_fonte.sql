{{
    config(
        alias="programa_fonte",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        SAFE_CAST(REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING) id_obra,
        SAFE_CAST(
            REGEXP_REPLACE(cd_prg_trab, r'\.0$', '') AS STRING
        ) id_programa_trabalho,
        SAFE_CAST(programa_trabalho AS STRING) programa_trabalho,
        SAFE_CAST(
            REGEXP_REPLACE(cd_fonte_recurso, r'\.0$', '') AS STRING
        ) id_fonte_recurso,
        SAFE_CAST(fonte_recurso AS STRING) fonte_recurso,
        SAFE_CAST(
            REGEXP_REPLACE(cd_natureza_dsp, r'\.0$', '') AS STRING
        ) id_natureza_despesa,
        SAFE_CAST(natureza_despesa AS STRING) natureza_despesa,
FROM {{ source('brutos_siscob_staging', 'programa_fonte') }} AS t