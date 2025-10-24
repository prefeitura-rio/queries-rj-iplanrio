{{
    config(
        alias="protesto_cda",
        materialized="table",
        tags=["raw", "devedor_cda", "cda", "devedor"],
        description="Tabela que contém os registros de protestos vinculados às certidões de dívida ativa (CDA), atualizados pelos cartórios."
    )
}}

SELECT safe_cast(idProtesto as int64) as id_protesto,
    safe_cast(numCDA as int64) as id_certidao_divida_ativa,
    safe_cast(numeroCartorio as string) as numero_cartorio,
    safe_cast(numeroProtocolo as string) as numero_protocolo,
    safe_cast(dataProtocolo as date) as data_protocolo,
    safe_cast(numeroTitulo as string) as numero_titulo,
    safe_cast(tipoOcorrencia as string) as codigo_tipo_ocorrencia,
    ifnull(b.descricao_mensagem_protesto, 'Não informado') as descricao_mensagem_protesto,
    safe_cast(dataOcorrencia as date) as data_ocorrencia,
    safe_cast(dataEmissaoTitulo as date) as data_emissao_cda,
    safe_cast(dataVencimentoTitulo as date) as data_vencimento_guia,
    safe_cast(valorTitulo as numeric) as valor_cda,
    safe_cast(nomeDevedor as string) as nome_devedor_protestado,
    safe_cast(tipoDocumentoDevedor as int64) as tipo_documento_devedor,
    case safe_cast(ifnull(tipoDocumentoDevedor, '0') as int64)
        when 1 then 'CNPJ'
        when 2 then 'CPF'
        when 0 then 'Não informado'
        else 'Não classificado'
    end as nome_tipo_documento_devedor,
    safe_cast(numeroDocumentoDevedor as string) as numero_documento_devedor,
    safe_cast(enderecoDevedor as string) as endereco_devedor,
    safe_cast(numEnderecoDevedor as string) as numero_porta_endereco,
    safe_cast(complementoEndereco as string) as complemento_endereco,
    safe_cast(bairroDevedor as string) as bairro_devedor,
    safe_cast(cepDevedor as string) as cep_devedor,
    safe_cast(cidadeDevedor as string) as cidade_devedor,
    safe_cast(ufDevedor as string) as uf_devedor,
    safe_cast(numeroGuiaPagamento as numeric) as numero_guia_pagamento,
    safe_cast(datInclusao as date) as data_inclusao,
    a._airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
FROM {{ source('brutos_divida_ativa_staging', 'ProtestosCDAs') }} a
left join {{ ref('raw_divida_ativa_mensagem_protesto')}} b on b.codigo_mensagem_protesto = a.tipoOcorrencia and b.tipo_servico = 'QC'