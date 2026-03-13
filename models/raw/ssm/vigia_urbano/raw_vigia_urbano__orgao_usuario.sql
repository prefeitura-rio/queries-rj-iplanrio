{{
    config(
        alias="orgao_usuario",
        description="Tabela de Relacionamento entre Orgãos e Usuários do sistema Vigia Urbano.",
    )
}}

SELECT
    safe_cast(orgaoUsuarioID AS int64) AS id_orgao_usuario,
    safe_cast(orgaoID AS int64) AS id_orgao,
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'OrgaoUsuario') }}
