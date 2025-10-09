{{
    config(
        alias="processo",
        description="Dados brutos de processos do SICOP (VW_PROCESSO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
select
  safe_cast(NUM_PROCESSO as string)                                    as id_processo,
  safe_cast(DATA_PROCESSO as date)                                     as data_processo,

  -- Documento
  safe_cast(TIPO_DOCUMENTO as string)                                  as documento,
  safe_cast(DOCUMENTO as string)                                       as nome_documento,
  safe_cast(ORGAO_DOCUMENTO as string)                                 as orgao_documento,
  safe_cast(TIPO_DOCTO as int64)                                       as tipo_documento,
  safe_cast(TIPO as string)                                            as nome_tipo_documento,

  -- Assunto  
  safe_cast(CODIGO_ASSUNTO as string)                                  as codigo_assunto,
  safe_cast(DESCRICAO_ASSUNTO as string)                               as descricao_assunto,

  -- Requerente e endereço/contato  
  safe_cast(REQUERENTE as string)                                      as requerente,
  safe_cast(AUTO_INFRACAO as string)                                   as auto_infracao,
  safe_cast(CODIGO_LOGRADOURO as string)                               as logradouro,
  safe_cast(NUMERO_PORTA_REQUERENTE as int64)                          as numero_porta,
  safe_cast(COMPLEMENTO_REQUERENTE as string)                          as complemento,
  safe_cast(CEP as string)                                             as cep,
  safe_cast(BAIRRO as string)                                          as bairro,
  safe_cast(DDI as string)                                             as ddi,
  safe_cast(DDD as string)                                             as ddd,
  safe_cast(TELEFONE as string)                                        as telefone,
  safe_cast(RAMAL as string)                                           as ramal,
  safe_cast(EMAIL as string)                                           as email,

  -- Outros identificadores
  safe_cast(NUM_PROCESSO_JUDICIAL as string)                           as numero_processo_judicial,
  safe_cast(INS_IMOB_INICIAL as string)                                as inscricao_imobiliaria_inicial,
  safe_cast(INS_IMOB_FINAL as string)                                  as inscricao_imobiliaria_final,

  -- Status de documentação
  safe_cast(DOC_ARQUIVADO as string)                                   as documento_arquivado,
  safe_cast(DOC_MICROFILMADO as string)                                as documento_microfilmado,
  safe_cast(DOC_ELIMINADO as string)                                   as documento_eliminado,
  safe_cast(DOC_EXTRAVIADO as string)                                  as documento_extraviado,
  safe_cast(DOC_DISTRIBUIDO as string)                                 as documento_distribuido,

  -- Status do registro
  safe_cast(STA_REGISTRO as string)                                    as status_registro,
  safe_cast(STA_DIG_REGISTRO as string)                                as status_digitacao_registro,
  safe_cast(DOC_REMISSIVO as string)                                   as documento_remissivo,
  safe_cast(ORG_ORIGEM as string)                                      as orgao_origem,
  safe_cast(ORG_TRANSCRITOR as string)                                 as orgao_transcritor,

  -- Metadados de arquivo/localização 
  safe_cast(PASTA_DOCUMENTO as string)                                 as pasta_localizacao_documento,
  safe_cast(MUDANCA_REGISTRO as string)                                as mudanca_registro,
  
  -- Medidas/quantidades  
  safe_cast(QTD_VOLUMES as int64)                                      as quantidade_volume,
  safe_cast(NUM_VOLUME as int64)                                       as numero_volume,

  -- Auditoria de alteração 
  safe_cast(DATA_ALTERACAO as date)                                    as data_alteracao,
  safe_cast(HORA_ALTERACAO as time)                                    as hora_alteracao,
  safe_cast(MAT_ALTERACAO as string)                                   as matricula_alteracao,
  safe_cast(ORG_ALTERACAO as string)                                   as orgao_alteracao,
  safe_cast(ORG_DESTINO as string)                                     as orgao_destino,
  safe_cast(STA_PRINCIPAL as string)                                   as status_principal
from {{ source('sicop', 'VW_PROCESSO_DLK') }}
