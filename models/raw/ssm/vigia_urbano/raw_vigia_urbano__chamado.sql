{{
    config(
        alias="chamado",
        description="Tabela de chamados do sistema Vigia Urbano."
    )
}}

SELECT
    SAFE_CAST(chamadoid AS STRING) AS id_chamado,
    SAFE_CAST(a.chamado AS STRING) AS chamado,
    SAFE_CAST(observacao AS STRING) AS observacao,
    SAFE_CAST(prazo AS DATE) AS prazo,
    SAFE_CAST(status AS INT64) AS status,
    SAFE_CAST(subareaid AS STRING) AS id_subarea,
    SAFE_CAST(categoriaenum AS INT64) AS categoria_enum,
    SAFE_CAST(orgaoresponsavelid AS STRING) AS id_orgao_responsavel,
    SAFE_CAST(usuarioregistroid AS STRING) AS id_usuario_registro,
    SAFE_CAST(dataregistro AS DATETIME) AS data_registro,
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) AS datalake_loaded_at,
    CURRENT_DATETIME('America/Sao_Paulo') AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'chamado') }} as a