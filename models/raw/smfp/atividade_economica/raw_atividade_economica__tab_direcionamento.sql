{{
    config(
        alias='tab_direcionamento',
        description='Tabela de dimensão com informações sobre direcionamentos'
    )
}}

SELECT
    SAFE_CAST(ID_Direcionamento AS STRING) AS ID_Direcionamento,
    SAFE_CAST(DSC_Direcionamento AS STRING) AS DSC_Direcionamento,
    _prefect_extracted_at as loaded_at,

FROM {{ source('atividade_economica_staging', 'tab_direcionamento') }}