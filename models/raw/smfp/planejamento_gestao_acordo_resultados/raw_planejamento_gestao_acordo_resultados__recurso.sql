{{
    config(
        alias='recurso'
    )
}}

SELECT
    SAFE_CAST (
        SAFE_CAST (SAFE_CAST (cod_recurso AS FLOAT64) AS INT64) AS STRING
    ) AS id_recurso,
    SAFE_CAST (ano_acordo_referencia_recurso AS INT64) AS ano,
    SAFE_CAST (orgao_solicitante AS STRING) AS orgao,
    SAFE_CAST (
        SAFE_CAST (SAFE_CAST (cod_meta AS FLOAT64) AS INT64) AS STRING
    ) AS codigo_meta,
    SAFE_CAST (orgao_acordo_egpweb AS STRING) AS orgao_egpweb,
    SAFE_CAST (oficio AS STRING) AS oficio,
    SAFE_CAST (codigo_processo_rio AS STRING) AS codigo_processorio,
    SAFE_CAST (data_solicitacao AS DATE FORMAT 'DD/MM/YYYY') AS data_solicitacao,
    SAFE_CAST (meta AS STRING) AS descricao_meta,
    SAFE_CAST (pedido AS STRING) AS pedido_recurso,
    SAFE_CAST (recomendacao_egp AS STRING) AS recomendacao_egp,
    SAFE_CAST (avaliacao_egp AS BOOL) AS avaliacao_egp,
    SAFE_CAST (SAFE_CAST (tipo_recurso AS FLOAT64) AS INT64) AS tipo_recurso,
    SAFE_CAST (decisao_prefeito AS BOOL) AS indicador_decisao_recurso
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'recurso') }}
