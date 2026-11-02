{{
    config(
        alias="usuario",
        description="Tabela de Usuários do sistema Vigia Urbano.",
    )
}}

SELECT
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe_cast(TRIM(CAST(nome AS STRING)) AS string) AS nome_usuario,
    safe_cast(TRIM(CAST(cpf AS STRING)) AS string) AS cpf_usuario,
    safe_cast(TRIM(CAST(email AS STRING)) AS string) AS email_usuario,
    safe_cast(TRIM(CAST(matricula AS STRING)) AS string) AS matricula_usuario,
    safe_cast(alterarSenha AS bool) AS flag_alterar_senha,
    safe_cast(ativo AS bool) AS flag_usuario_ativo,
    safe_cast(termoResponsabilidade AS bool) AS flag_termo_responsabilidade,
    safe_cast(_prefect_extracted_at as datetime) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'Usuario') }}
