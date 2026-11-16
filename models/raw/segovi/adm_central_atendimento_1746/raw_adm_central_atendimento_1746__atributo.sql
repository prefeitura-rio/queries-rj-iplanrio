{{
    config(
        alias='atributo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(cd_atributo_comum AS STRING) AS cd_atributo_comum,
    SAFE_CAST(ds_expressao AS STRING) AS ds_expressao,
    SAFE_CAST(ds_instrucao AS STRING) AS ds_instrucao,
    SAFE_CAST(ds_valores AS STRING) AS ds_valores,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_reposta_numerica AS BOOL) AS fl_reposta_numerica,
    SAFE_CAST(ic_interface_atributo AS STRING) AS ic_interface_atributo,
    SAFE_CAST(ic_tipo_atributo AS STRING) AS ic_tipo_atributo,
    SAFE_CAST(id_atributo AS INT64) AS id_atributo,
    SAFE_CAST(no_amigavel AS STRING) AS no_amigavel,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_atributo') }}
