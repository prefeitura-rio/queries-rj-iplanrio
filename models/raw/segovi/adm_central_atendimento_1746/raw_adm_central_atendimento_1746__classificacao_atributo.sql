{{
    config(
        alias='classificacao_atributo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_equivalente AS BOOL) AS fl_equivalente,
    SAFE_CAST(fl_obrigatorio AS BOOL) AS fl_obrigatorio,
    SAFE_CAST(fl_visivel AS BOOL) AS fl_visivel,
    SAFE_CAST(id_atributo_fk AS INT64) AS id_atributo_fk,
    SAFE_CAST(id_classificacao_atributo AS INT64) AS id_classificacao_atributo,
    SAFE_CAST(id_classificacao_fk AS INT64) AS id_classificacao_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_classificacao_atributo') }}
