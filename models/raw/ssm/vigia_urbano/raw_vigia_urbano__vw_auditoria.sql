{{
    config(
        alias="auditoria",
        description="Registros de auditoria do sistema Vigia Urbano, contendo informações sobre ações realizadas pelos usuários no sistema.",
    )
}}

SELECT
    SAFE_CAST(auditoriaID AS INT64) AS id_auditoria,
    SAFE_CAST(usuarioID AS INT64) AS id_usuario,
    SAFE_CAST(dataHora AS DATETIME) AS data_hora,
    SAFE_CAST(dadosJson AS STRING) AS dados_json,
    SAFE_CAST(TRIM(evento_descricao) AS STRING) AS evento_descricao,
    SAFE_CAST(TRIM(tela_descricao) AS STRING) AS tela_descricao,
    SAFE_CAST(TRIM(usuarioNome) AS STRING) AS usuario_nome,
    SAFE_CAST(TRIM(usuarioMatricula) AS STRING) AS usuario_matricula,
    SAFE_CAST(CURRENT_TIMESTAMP() AS DATETIME) AS datalake_transformed_at
FROM {{ source('brutos_vigia_urbano_staging', 'vw_auditoria') }}
