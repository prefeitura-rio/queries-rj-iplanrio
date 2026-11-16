{{
    config(
        alias='categoria',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(cd_externo AS STRING) AS cd_externo,
    SAFE_CAST(fl_apenas_ouvidor AS BOOL) AS fl_apenas_ouvidor,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_permite_siso AS BOOL) AS fl_permite_siso,
    SAFE_CAST(ic_tratamento AS STRING) AS ic_tratamento,
    SAFE_CAST(id_categoria AS INT64) AS id_categoria,
    SAFE_CAST(no_categoria AS STRING) AS no_categoria,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_categoria') }}
