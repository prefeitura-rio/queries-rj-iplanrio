{{
    config(
        alias='tab_cae',
        description='Tabela de dimensão com códigos CAE (Classificação de Atividades Econômicas)'
    )
}}

SELECT
    SAFE_CAST(ID_CAE AS STRING) AS ID_CAE,
    SAFE_CAST(DSC_CAE AS STRING) AS DSC_CAE,
    SAFE_CAST(ID_TipoAtividade AS STRING) AS ID_TipoAtividade,
    SAFE_CAST(DSC_TipoAtividade AS STRING) AS DSC_TipoAtividade

FROM {{ source('atividade_economica_staging', 'tab_cae') }}