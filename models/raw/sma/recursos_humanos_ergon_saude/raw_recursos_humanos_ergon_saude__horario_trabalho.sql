{{
    config(
        alias='horario_trabalho',
    )
}}

SELECT
    SAFE_CAST(CODIGO AS STRING) AS id_horario,
    SAFE_CAST(DESCRICAO AS STRING) AS descricao,
    SAFE_CAST(PONTLEI AS STRING) AS publicacao_diario_oficial,
    SAFE_CAST(CARGA_HR_MES AS STRING) AS carga_horaria_mes,
    SAFE_CAST(CARGA_HR_SEMANA AS STRING) AS carga_horaria_semana,
    SAFE_CAST(CARGA_HR_DIA AS STRING) AS carga_horaria_dia,
    SAFE_CAST(ID_REG AS STRING) AS id_registro,
FROM {{ source('brutos_ergon_saude_staging', 'VW_DLK_ERG_HORARIO_TRAB_') }} AS t