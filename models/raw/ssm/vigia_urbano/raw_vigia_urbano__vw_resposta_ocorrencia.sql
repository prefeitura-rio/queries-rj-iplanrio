{{
    config(
        alias="resposta_ocorrencia",
        description="Respostas de ocorrências do sistema Vigia Urbano, contendo informações sobre ocorrências registradas e suas respostas.",
    )
}}

SELECT
    SAFE_CAST(Id AS INT64) AS id_resposta_ocorrencia,
    SAFE_CAST(TRIM(cpfUsuario) AS STRING) AS cpf_usuario,
    SAFE_CAST(TRIM(nomeUsuario) AS STRING) AS nome_usuario,
    SAFE_CAST(dataHoraRegistro AS DATETIME) AS data_hora_registro,
    SAFE_CAST(TRIM(logradouro) AS STRING) AS logradouro,
    SAFE_CAST(TRIM(numeroPorta) AS STRING) AS numero_porta,
    SAFE_CAST(bairroID AS INT64) AS id_bairro,
    SAFE_CAST(TRIM(referencia) AS STRING) AS referencia,
    SAFE_CAST(coordenadaX AS FLOAT64) AS coordenada_x,
    SAFE_CAST(coordenadaY AS FLOAT64) AS coordenada_y,
    SAFE_CAST(coordenadaXDispositivo AS FLOAT64) AS coordenada_x_dispositivo,
    SAFE_CAST(coordenadaYDispositivo AS FLOAT64) AS coordenada_y_dispositivo,
    SAFE_CAST(TRIM(observacao) AS STRING) AS observacao,
    SAFE_CAST(TRIM(TPTFOCUP_descricao) AS STRING) AS tipo_foco_ocupacao_descricao,
    SAFE_CAST(excluido AS BOOL) AS excluido,
    SAFE_CAST(TRIM(enderecoInformado) AS STRING) AS endereco_informado,
    SAFE_CAST(TRIM(caminhoImagem) AS STRING) AS caminho_imagem,
    SAFE_CAST(ocorrenciaID AS INT64) AS id_ocorrencia,
    SAFE_CAST(TRIM(bairroNome) AS STRING) AS bairro_nome,
    SAFE_CAST(TRIM(ocorrenciaDescricao) AS STRING) AS ocorrencia_descricao,
    SAFE_CAST(CURRENT_TIMESTAMP() AS DATETIME) AS datalake_transformed_at
FROM {{ source('brutos_vigia_urbano_staging', 'vw_respostaocorrencia') }}
