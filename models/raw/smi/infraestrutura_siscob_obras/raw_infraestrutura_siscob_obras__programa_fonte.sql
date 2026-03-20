{{
    config(
        alias="programa_fonte",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT 
    DISTINCT
        {{ clean_and_cast('cd_obra', 'string') }} id_obra,
        {{ clean_and_cast('cd_prg_trab', 'string') }} id_programa_trabalho,
        SAFE_CAST(programa_trabalho AS STRING) programa_trabalho,
        {{ clean_and_cast('cd_fonte_recurso', 'string') }} id_fonte_recurso,
        SAFE_CAST(fonte_recurso AS STRING) fonte_recurso,
        {{ clean_and_cast('cd_natureza_dsp', 'string') }} id_natureza_despesa,
        SAFE_CAST(natureza_despesa AS STRING) natureza_despesa,
FROM {{ source('brutos_siscob_staging', 'programa_fonte') }} AS t