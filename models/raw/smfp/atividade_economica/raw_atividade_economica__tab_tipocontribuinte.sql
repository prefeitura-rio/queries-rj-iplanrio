{{
    config(
        alias='tab_tipocontribuinte',
        description='Tabela de dimensão com tipos de contribuinte'
    )
}}

SELECT
    SAFE_CAST(ID_TipoContribuint AS STRING) AS ID_TipoContribuint,
    SAFE_CAST(DSC_TipoContribuint AS STRING) AS DSC_TipoContribuint,
    _prefect_extracted_at as loaded_at,
FROM {{ source('atividade_economica_staging', 'tab_tipocontribuinte_tipocontribuint') }}