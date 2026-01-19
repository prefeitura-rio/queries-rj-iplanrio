{{
    config(
        alias="processo",
        description="Dados brutos de processos do SICOP (VW_PROCESSO_DLK)"
    )
}}



-- Conversões e padronização de nomes conforme guia de estilo
select
  safe_cast(id_processo as string)                          as id_processo,
  safe_cast(CONCAT( SUBSTR(data_processo,7,4),'-', SUBSTR(data_processo,4,2) ,'-', SUBSTR(data_processo,1,2) ) as date)         as data_processo,

  -- Documento
  safe_cast(documento as string)                            as documento,
  safe_cast(nome_documento as string)                       as nome_documento,
  safe_cast(orgao_documento as string)                      as orgao_documento,
  safe_cast(tipo_documento as int64)                        as tipo_documento,
  safe_cast(nome_tipo_documento as string)                  as nome_tipo_documento,

  -- Assunto  
  safe_cast(codigo_assunto as string)                       as codigo_assunto,
  safe_cast(descricao_assunto as string)                    as descricao_assunto,

  -- Requerente e endereço/contato  
  safe_cast(requerente as string)                           as requerente,
  safe_cast(auto_infracao as string)                        as auto_infracao,
  safe_cast(logradouro as string)                           as logradouro,
  safe_cast(numero_porta as int64)                          as numero_porta,
  safe_cast(complemento as string)                          as complemento,
  safe_cast(cep as string)                                  as cep,
  safe_cast(bairro as string)                               as bairro,
  safe_cast(ddi as string)                                  as ddi,
  safe_cast(ddd as string)                                  as ddd,
  safe_cast(telefone as string)                             as telefone,
  safe_cast(ramal as string)                                as ramal,
  safe_cast(email as string)                                as email,

  -- Outros identificadores
  safe_cast(numero_processo_judicial as string)             as numero_processo_judicial,
  safe_cast(inscricao_imobiliaria_inicial as string)        as inscricao_imobiliaria_inicial,
  safe_cast(inscricao_imobiliaria_final as string)          as inscricao_imobiliaria_final,

  -- Status de documentação
  safe_cast(documento_arquivado as string)                  as documento_arquivado,
  safe_cast(documento_microfilmado as string)               as documento_microfilmado,
  safe_cast(documento_eliminado as string)                  as documento_eliminado,
  safe_cast(documento_extraviado as string)                 as documento_extraviado,
  safe_cast(documento_distribuido as string)                as documento_distribuido,

  -- Status do registro
  safe_cast(status_registro as string)                      as status_registro,
  safe_cast(status_digitacao_registro as string)            as status_digitacao_registro,
  safe_cast(documento_remissivo as string)                  as documento_remissivo,
  safe_cast(orgao_origem as string)                         as orgao_origem,
  safe_cast(orgao_transcritor as string)                    as orgao_transcritor,

  -- Metadados de arquivo/localização 
  safe_cast(pasta_localizacao_documento as string)          as pasta_localizacao_documento,
  safe_cast(mudanca_registro as string)                     as mudanca_registro,
  
  -- Medidas/quantidades  
  safe_cast(quantidade_volume as int64)                     as quantidade_volume,
  safe_cast(numero_volume as int64)                         as numero_volume,

  -- Auditoria de alteração 
  safe_cast(CONCAT( SUBSTR(data_alteracao,7,4),'-', SUBSTR(data_alteracao,4,2) ,'-', SUBSTR(data_alteracao,1,2) ) as date)         as data_alteracao,
  PARSE_TIME("%H:%M", hora_alteracao) as hora_alteracao,
  --safe_cast(hora_alteracao as time)                         as hora_alteracao,
  safe_cast(matricula_alteracao as string)                  as matricula_alteracao,
  safe_cast(orgao_alteracao as string)                      as orgao_alteracao,
  safe_cast(orgao_destino as string)                        as orgao_destino,
  safe_cast(status_principal as string)                     as status_principal,
  safe_cast(SUBSTR(_prefect_extracted_at,1,10) as date)      as datalake_transformed_at  
from {{ source("brutos_sicop_staging","processo") }}
