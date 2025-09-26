{{
  config(
    alias='cpf',
    materialized='view',
  )
}}

select
  -- Identificador principal
  cpf,
  
  -- Dados pessoais
  nome,
  nome_social,
  mae_nome,
  
  -- Datas importantes
  nascimento_data,
  inscricao_data,
  atualizacao_data,
  
  -- Status e demografia
  situacao_cadastral_tipo,
  sexo,
  obito_ano,
  estrangeiro_indicador,
  residente_exterior_indicador,
  
  -- Informações de contato
  contato,
  contato.telefone as contato_telefone,
  contato.telefone.ddi as contato_telefone_ddi,
  contato.telefone.ddd as contato_telefone_ddd,
  contato.telefone.numero as contato_telefone_numero,
  contato.email as contato_email,
  
  -- Endereço completo
  endereco,
  endereco.cep as endereco_cep,
  endereco.uf as endereco_uf,
  endereco.municipio as endereco_municipio,
  endereco.bairro as endereco_bairro,
  endereco.tipo_logradouro as endereco_tipo_logradouro,
  endereco.logradouro as endereco_logradouro,
  endereco.numero as endereco_numero,
  endereco.complemento as endereco_complemento,
  
  -- Local de nascimento
  nascimento_local,
  nascimento_local.uf as nascimento_uf,
  nascimento_local.municipio as nascimento_municipio,
  nascimento_local.pais as nascimento_pais,
  nascimento_local.id_municipio as nascimento_id_municipio,
  nascimento_local.id_pais as nascimento_id_pais,
  
  -- Ocupação
  ocupacao,
  ocupacao.nome as ocupacao_nome,
  ocupacao.id as ocupacao_id,
  ocupacao.id_natureza as ocupacao_id_natureza,
  ocupacao.id_ua as ocupacao_id_ua,
  
  -- Metadados
  metadados,
  metadados.ano_exercicio as metadados_ano_exercicio,
  metadados.version as metadados_version,
  metadados.tipo as metadados_tipo,
  metadados.timestamp as metadados_timestamp,
  
  -- Campos técnicos do Airbyte
  airbyte,
  airbyte.seq as airbyte_seq,
  airbyte.last_seq as airbyte_last_seq,
  airbyte.airbyte_raw_id as airbyte_raw_id,
  airbyte.airbyte_extracted_at as airbyte_extracted_at,
  airbyte.airbyte_meta as airbyte_meta,
  airbyte.airbyte_meta.changes as airbyte_changes,
  airbyte.airbyte_meta.sync_id as airbyte_sync_id,
  airbyte.airbyte_generation_id as airbyte_generation_id,
  
  -- Coluna de partição
  cpf_particao

from {{ ref('raw_bcadastro_cpf') }}
