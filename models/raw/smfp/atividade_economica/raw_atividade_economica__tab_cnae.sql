{{
    config(
        alias='tab_cnae',
        description='Tabela de dimensão com códigos CNAE (Classificação Nacional de Atividades Econômicas)'
    )
}}

SELECT
    SAFE_CAST(ID_CNAE AS STRING) AS ID_CNAE,
    SAFE_CAST(DSC_CNAE AS STRING) AS DSC_CNAE,
    _prefect_extracted_at as loaded_at,

FROM {{ source('atividade_economica_staging', 'tab_cnae') }}