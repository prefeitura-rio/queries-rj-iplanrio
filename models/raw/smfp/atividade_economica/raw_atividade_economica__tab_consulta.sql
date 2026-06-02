{{
    config(
        alias='tab_consulta',
        description='Tabela de dimensão com informações detalhadas sobre consultas prévias'
    )
}}

SELECT
    SAFE_CAST(ID_Consulta AS STRING) AS ID_Consulta,
    SAFE_CAST(DSC_Consulta AS STRING) AS DSC_Consulta,
    SAFE_CAST(DSC_Endereco_cp AS STRING) AS DSC_Endereco_cp,
    SAFE_CAST(DSC_Bairro_cp AS STRING) AS DSC_Bairro_cp,
    SAFE_CAST(DSC_Zoneamento_cp AS STRING) AS DSC_Zoneamento_cp,
    SAFE_CAST(DSC_CodeConsulta AS FLOAT64) AS DSC_CodeConsulta,
    SAFE_CAST(DSC_IRLF_cp AS STRING) AS DSC_IRLF_cp,
    SAFE_CAST(DSC_StatusCPL_cp AS STRING) AS DSC_StatusCPL_cp,
    SAFE_CAST(DSC_TipoAnalise_cp AS STRING) AS DSC_TipoAnalise_cp,
    SAFE_CAST(DSC_Status_cp AS STRING) AS DSC_Status_cp,
    _prefect_extracted_at as loaded_at,

FROM {{ source('atividade_economica_staging', 'tab_consulta') }}