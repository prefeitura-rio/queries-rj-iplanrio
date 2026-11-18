{{
    config(
        alias="resposta_ocorrencia",
        description="Respostas de ocorrências do sistema Vigia Urbano, contendo informações sobre ocorrências registradas e suas respostas.",
    )
}}


with resposta_ocorrencia AS (SELECT 
    Id,
    dataHoraRegistro,
    logradouro,
    numeroPorta,
    referencia,
    coordenadaX,
    coordenadaY,
    coordenadaXDispositivo,
    coordenadaYDispositivo,
    observacao,
    CASE 
    WHEN {{ vigia_flag("excluido") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("excluido") }} = 0 THEN FALSE
    ELSE NULL 
    END as excluido,
    CASE 
    WHEN {{ vigia_flag("enderecoInformado") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("enderecoInformado") }} = 0 THEN FALSE
    ELSE NULL 
    END as enderecoInformado,
    caminhoImagem,
    CASE 
    WHEN {{ vigia_flag("valido") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("valido") }} = 0 THEN FALSE
    ELSE NULL 
    END as valido,
    bairroID,
    bairroNome,
    tipoPessoa,
    subareaid,
    subarea,
    tipoPessoaDescricao,
    ocupacaoPessoa,
    ocupacaoPessoaDescricao,
    tipoFrequencia,
    tipoFrequenciaDescricao,
    ocupacaoDrogas,
    ocupacaoDrogasDescricao,
    itemPraca,
    itemPracaDescricao,
    ocorrenciaID,
    ocorrenciaDescricao,
    CASE 
    WHEN {{ vigia_flag("ocorrenciaAtivo") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("ocorrenciaAtivo") }} = 0 THEN FALSE
    ELSE NULL 
    END as ocorrenciaAtivo,
    orgaoResponsavel,
    quemOpera,
    servico1746,
    ocorrenciaInformacao,
    ocorrenciaOrgaoID,
    ocorrenciaOrgaoNome,
    ocorrenciaOrgaoCodigo,
    categoriaID,
    categoriaNome,
    CASE 
    WHEN {{ vigia_flag("categoriaAtivo") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("categoriaAtivo") }} = 0 THEN FALSE
    ELSE NULL 
    END as categoriaAtivo,
    usuarioID,
    usuarioNome,
    usuarioCpf,
    usuarioEmail,
    usuarioMatricula,
    CASE 
    WHEN {{ vigia_flag("usuarioAtivo") }} = 1 THEN TRUE
    WHEN {{ vigia_flag("usuarioAtivo") }} = 0 THEN FALSE
    ELSE NULL 
    END as usuarioAtivo,
    usuarioPerfis,
    usuarioOrgaoNome,
    usuarioOrgaoCodigo,
    _prefect_extracted_at
    FROM {{ source('brutos_formulario_ocorrencia_staging', 'vw_RespostaOcorrencia') }})

SELECT
    safe_cast(Id AS int64) AS id_resposta_ocorrencia,
    safe_cast(dataHoraRegistro AS datetime) AS data_hora_registro,
    safe_cast(TRIM(CAST(logradouro AS STRING)) AS string) AS logradouro,
    safe_cast(TRIM(CAST(numeroPorta AS STRING)) AS string) AS numero_porta,
    safe_cast(TRIM(CAST(referencia AS STRING)) AS string) AS referencia,
    safe_cast(TRIM(CAST(coordenadaX AS STRING)) AS string) AS coordenada_x,
    safe_cast(TRIM(CAST(coordenadaY AS STRING)) AS string) AS coordenada_y,
    safe_cast(TRIM(CAST(coordenadaXDispositivo AS STRING)) AS string) AS coordenada_x_dispositivo,
    safe_cast(TRIM(CAST(coordenadaYDispositivo AS STRING)) AS string) AS coordenada_y_dispositivo,
    safe_cast(TRIM(CAST(observacao AS STRING)) AS string) AS observacao,
    safe_cast(excluido AS bool) AS excluido,
    safe_cast(enderecoInformado AS bool) AS endereco_informado,
    safe_cast(TRIM(CAST(caminhoImagem AS STRING)) AS string) AS caminho_imagem,
    safe_cast(valido AS bool) AS valido,
    safe_cast(bairroID AS int64) AS id_bairro,
    safe_cast(TRIM(CAST(bairroNome AS STRING)) AS string) AS bairro_nome,
    safe_cast(tipoPessoa AS int64) AS id_tipo_pessoa,
    safe_cast(REPLACE(cast(subareaid AS string), ".0", "") AS string) AS id_subarea,
    safe_cast(subarea AS string) AS subarea_nome,
    safe_cast(TRIM(CAST(tipoPessoaDescricao AS STRING)) AS string) AS tipo_pessoa_descricao,
    safe_cast(ocupacaoPessoa AS int64) AS id_ocupacao_pessoa,
    safe_cast(TRIM(CAST(ocupacaoPessoaDescricao AS STRING)) AS string) AS ocupacao_pessoa_descricao,
    safe_cast(tipoFrequencia AS int64) AS id_tipo_frequencia,
    safe_cast(TRIM(CAST(tipoFrequenciaDescricao AS STRING)) AS string) AS tipo_frequencia_descricao,
    safe_cast(ocupacaoDrogas AS int64) AS ocupacao_drogas,
    safe_cast(TRIM(CAST(ocupacaoDrogasDescricao AS STRING)) AS string) AS ocupacao_drogas_descricao,
    safe_cast(itemPraca AS int64) AS id_item_praca,
    safe_cast(TRIM(CAST(itemPracaDescricao AS STRING)) AS string) AS item_praca_descricao,
    safe_cast(ocorrenciaID AS int64) AS id_tipo_ocorrencia,
    safe_cast(TRIM(CAST(ocorrenciaDescricao AS STRING)) AS string) AS tipo_ocorrencia_descricao,
    safe_cast(ocorrenciaAtivo AS bool) AS tipo_ocorrencia_ativo,
    safe_cast(TRIM(CAST(orgaoResponsavel AS STRING)) AS string) AS orgao_responsavel,
    safe_cast(TRIM(CAST(quemOpera AS STRING)) AS string) AS quem_opera,
    safe_cast(TRIM(CAST(servico1746 AS STRING)) AS string) AS servico_1746,
    safe_cast(TRIM(CAST(ocorrenciaInformacao AS STRING)) AS string) AS ocorrencia_informacao,
    safe_cast(ocorrenciaOrgaoID AS int64) AS id_orgao_ocorrencia,
    safe_cast(TRIM(CAST(ocorrenciaOrgaoNome AS STRING)) AS string) AS ocorrencia_orgao_nome,
    safe_cast(TRIM(CAST(ocorrenciaOrgaoCodigo AS STRING)) AS string) AS codigo_ocorrencia_orgao,
    safe_cast(categoriaID AS int64) AS id_categoria,
    safe_cast(TRIM(CAST(categoriaNome AS STRING)) AS string) AS categoria_nome,
    safe_cast(categoriaAtivo AS bool) AS categoria_ativo,
    safe_cast(usuarioID AS int64) AS id_usuario,
    safe_cast(TRIM(CAST(usuarioNome AS STRING)) AS string) AS usuario_nome,
    safe_cast(TRIM(CAST(usuarioCpf AS STRING)) AS string) AS usuario_cpf,
    safe_cast(TRIM(CAST(usuarioEmail AS STRING)) AS string) AS usuario_email,
    safe_cast(TRIM(CAST(usuarioMatricula AS STRING)) AS string) AS usuario_matricula,
    safe_cast(usuarioAtivo AS bool) AS usuario_ativo,
    safe_cast(TRIM(CAST(usuarioPerfis AS STRING)) AS string) AS usuario_perfis,
    safe_cast(TRIM(CAST(usuarioOrgaoNome AS STRING)) AS string) AS usuario_orgao_nome,
    safe_cast(TRIM(CAST(usuarioOrgaoCodigo AS STRING)) AS string) AS codigo_orgao_usuario,
    safe_cast(_prefect_extracted_at as datetime) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM resposta_ocorrencia