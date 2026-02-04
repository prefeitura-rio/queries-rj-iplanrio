{{
    config(
        alias="auditoria",
        materialized='table',
        description="Registros de auditoria do sistema Vigia Urbano, contendo informações sobre ações realizadas pelos usuários no sistema.",
    )
}}

SELECT
    safe_cast(auditoriaID AS int64) AS id_auditoria,
    safe_cast(dataHora AS datetime) AS data_hora,
    safe_cast(dadosJson AS string) AS dados_json,
    safe_cast(telaEnum AS int64) AS tela_enum,
    safe_cast(TRIM(telaDescricao) AS string) AS tela_descricao,
    safe_cast(TRIM(evento) AS string) AS evento,
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
    safe_cast(SUBSTR(_prefect_extracted_at,1,10) as date)          as datalake_loaded_at ,
    safe_cast(CURRENT_TIMESTAMP() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'vw_Auditoria') }}



