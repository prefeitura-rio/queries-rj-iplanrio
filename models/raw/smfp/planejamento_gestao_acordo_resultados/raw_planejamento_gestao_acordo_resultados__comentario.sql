{{
    config(
        alias='comentario'
    )
}}

SELECT
    SAFE_CAST(chaveCamada AS STRING) as id_camada,
    SAFE_CAST(camadaMetaEnum AS STRING) as indice_camada,
    SAFE_CAST(ano AS INT64) as ano,
    SAFE_CAST(mes AS INT64) as mes,
    SAFE_CAST(comentario AS STRING) as comentario,
    SAFE_CAST(usuarioRegistroID AS STRING) as numero_analista,
    SAFE_CAST(ordem as STRING) as ordem
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'comentario') }}
