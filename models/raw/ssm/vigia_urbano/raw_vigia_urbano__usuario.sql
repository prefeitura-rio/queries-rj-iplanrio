{{
    config(
        alias="usuario",
        description="Tabela de Usuários do sistema Vigia Urbano.",
    )
}}

SELECT
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe_cast(TRIM(nome) AS string) AS nome_usuario,
    safe_cast(TRIM(cpf) AS string) AS cpf_usuario,
    safe_cast(TRIM(email) AS string) AS email_usuario,
    safe_cast(TRIM(matricula) AS string) AS matricula_usuario,
    safe_cast(alterarSenha AS bool) AS flag_alterar_senha,
    safe_cast(ativo AS bool) AS flag_usuario_ativo,
    safe_cast(termoResponsabilidade AS bool) AS flag_termo_responsabilidade,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'Usuario') }}
