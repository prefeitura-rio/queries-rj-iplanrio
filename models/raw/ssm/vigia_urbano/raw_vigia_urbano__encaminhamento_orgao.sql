{{
    config(
        alias="encaminhamento_orgao",
        description="Tabela de encaminhamentos de órgãos do sistema Vigia Urbano."
    )
}}

SELECT
    safe_cast(encaminhamentoID AS string) AS id_encaminhamento,
    safe_cast(chamadoID AS string) AS id_chamado,
    safe_cast(orgaoID AS string) AS id_orgao,
    safe_cast(status AS int64) AS status,
    safe_cast(atividade AS string) AS atividade,
    safe_cast(usuarioID AS string) AS id_usuario,
    safe_cast(dataRegistro AS datetime) AS data_registro,
    safe_cast(dataConclusao AS datetime) AS data_conclusao,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'EncaminhamentoOrgao') }}
