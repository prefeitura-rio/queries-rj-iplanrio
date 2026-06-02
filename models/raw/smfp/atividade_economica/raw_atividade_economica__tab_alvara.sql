{{
    config(
        alias='tab_alvara',
        description='Tabela de dimensão com informações detalhadas sobre os alvarás'
    )
}}

SELECT
    SAFE_CAST(ID_Alvara AS STRING) AS ID_Alvara,
    SAFE_CAST(DSC_Alvara AS STRING) AS DSC_Alvara,
    SAFE_CAST(DSC_Endereco AS STRING) AS DSC_Endereco,
    SAFE_CAST(DSC_Bairro AS STRING) AS DSC_Bairro,
    SAFE_CAST(DSC_Zoneamento AS STRING) AS DSC_Zoneamento,
    SAFE_CAST(DSC_IRLF AS STRING) AS DSC_IRLF,
    SAFE_CAST(DSC_TipoAnalise AS STRING) AS DSC_TipoAnalise,
    SAFE_CAST(DSC_TempoRespDia AS FLOAT64) AS DSC_TempoRespDia,
    SAFE_CAST(DSC_StatusIntermediario AS STRING) AS DSC_StatusIntermediario,
    SAFE_CAST(DSC_StatusCPL AS STRING) AS DSC_StatusCPL,
    SAFE_CAST(DSC_TempoRespMinuto AS FLOAT64) AS DSC_TempoRespMinuto,
    SAFE_CAST(DSC_TipoAlvara AS STRING) AS DSC_TipoAlvara,
    SAFE_CAST(DSC_TaxaOriginal AS FLOAT64) AS DSC_TaxaOriginal,
    SAFE_CAST(DSC_TaxaMulta AS FLOAT64) AS DSC_TaxaMulta,
    SAFE_CAST(DSC_TaxaMora AS FLOAT64) AS DSC_TaxaMora,
    SAFE_CAST(DSC_TaxaTotal AS FLOAT64) AS DSC_TaxaTotal,
    SAFE_CAST(DSC_IsentoTaxa AS STRING) AS DSC_IsentoTaxa,
    SAFE_CAST(DSC_Numero AS FLOAT64) AS DSC_Numero,
    SAFE_CAST(DSC_AlvaraLiberado AS STRING) AS DSC_AlvaraLiberado,
    _prefect_extracted_at as loaded_at,

FROM {{ source('atividade_economica_staging', 'tab_alvara') }}