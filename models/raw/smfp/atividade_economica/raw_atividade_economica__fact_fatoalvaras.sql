{{
    config(
        alias='fact_fatoalvaras',
        description='Tabela fato contendo métricas e chaves estrangeiras relacionadas aos alvarás emitidos'
    )
}}

SELECT
    SAFE_CAST(ID_Alvara AS STRING) AS ID_Alvara,
    SAFE_CAST(Quantidade AS FLOAT64) AS Quantidade,
    SAFE_CAST(ID_AtvProcesso AS STRING) AS ID_AtvProcesso,
    SAFE_CAST(ID_CAE AS STRING) AS ID_CAE,
    SAFE_CAST(ID_CNAE AS STRING) AS ID_CNAE,
    SAFE_CAST(ID_DiaDeferimento AS STRING) AS ID_DiaDeferimento,
    SAFE_CAST(ID_DiaSolicitacao AS STRING) AS ID_DiaSolicitacao,
    SAFE_CAST(ID_DiaTaxaPagamen AS STRING) AS ID_DiaTaxaPagamen,
    SAFE_CAST(ID_Direcionamento AS STRING) AS ID_Direcionamento,
    SAFE_CAST(ID_TipoContribuint AS STRING) AS ID_TipoContribuint,
    SAFE_CAST(ID_TipoSolicitacao AS STRING) AS ID_TipoSolicitacao

FROM {{ source('atividade_economica_staging', 'fact_fatoalvaras') }}