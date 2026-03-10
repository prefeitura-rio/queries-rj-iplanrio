{{
    config(
        alias="perfil_usuario",
        description="Tabela de Perfis dos Usuários do sistema Vigia Urbano.",
    )
}}

SELECT
    safe_cast(perfilUsuarioID AS int64) AS id_perfil_usuario,
    safe_cast(perfilEnum AS int64) AS id_perfil_enum,
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe_cast(usuarioRegistroID AS int64) AS id_usuario_registro,
    safe_cast(dataregistro AS datetime) AS data_registro,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'PerfilUsuario') }}
