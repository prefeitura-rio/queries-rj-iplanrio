{{
    config(
        alias='tab_tiposolicitacao',
        description='Tabela de dimensão com tipos de solicitação'
    )
}}

SELECT
    SAFE_CAST(ID_TipoSolicitacao AS STRING) AS ID_TipoSolicitacao,
    SAFE_CAST(DSC_TipoSolicitacao AS STRING) AS DSC_TipoSolicitacao

FROM {{ source('atividade_economica_staging', 'tab_tiposolicitacao') }}