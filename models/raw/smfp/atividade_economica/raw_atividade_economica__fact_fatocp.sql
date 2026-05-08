{{
    config(
        alias='fact_fatocp',
        description='Tabela fato contendo métricas e chaves estrangeiras relacionadas às consultas prévias'
    )
}}

SELECT
    SAFE_CAST(ID_AtvProcesso AS STRING) AS ID_AtvProcesso,
    SAFE_CAST(Quantidade_cp AS FLOAT64) AS Quantidade_cp,
    SAFE_CAST(ID_CAE AS STRING) AS ID_CAE,
    SAFE_CAST(ID_CNAE AS STRING) AS ID_CNAE,
    SAFE_CAST(ID_Consulta AS STRING) AS ID_Consulta,
    SAFE_CAST(ID_DiaInicial AS STRING) AS ID_DiaInicial,
    SAFE_CAST(ID_Direcionamento AS STRING) AS ID_Direcionamento,
    SAFE_CAST(ID_TipoContribuint AS STRING) AS ID_TipoContribuint,
    SAFE_CAST(ID_TipoSolicitacao AS STRING) AS ID_TipoSolicitacao

FROM {{ source('atividade_economica_staging', 'fact_fatocp') }}