{{
    config(
        alias="auditoria",
        description="Registros de auditoria do sistema Vigia Urbano, contendo informocorrências sobre ocorrências realizadas pelos usuários no sistema.",
    )
}}

SELECT
    safe_cast(auditoriaID AS int64) AS id_auditoria,
    safe_cast(dataHora AS datetime) AS data_hora_auditoria,
    safe_cast(dadosJson AS string) AS dados_json_auditoria,
    safe_cast(telaEnum AS int64) AS tela_enum,
    safe_cast(TRIM(telaDescricao) AS string) AS tela_descricao,
    safe_cast(evento AS int64) AS id_evento,
    safe_cast(TRIM(eventoDescricao) AS string) AS evento_descricao,
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe_cast(TRIM(usuarioNome) AS string) AS usuario_nome,
    safe_cast(TRIM(usuarioCpf) AS string) AS usuario_cpf,
    safe_cast(TRIM(usuarioMatricula) AS string) AS usuario_matricula,
    safe_cast(TRIM(usuarioEmail) AS string) AS usuario_email,
    safe_cast(usuarioAtivo AS bool) AS usuario_ativo,
    safe_cast(TRIM(usuarioPerfis) AS string) AS usuario_perfis,
    safe_cast(TRIM(usuarioOrgaoNome) AS string) AS usuario_orgao_nome,
    safe_cast(TRIM(usuarioOrgaoCodigo) AS string) AS usuario_orgao_codigo,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'vw_Auditoria') }}



