{{
  config(
    alias='cnpj',
    materialized='view',
  )
}}

select
  -- Identificador principal
  cnpj,
  
  -- Dados básicos da empresa
  razao_social,
  nome_fantasia,
  capital_social,
  cnae_fiscal,
  cnae_secundarias,
  nire,
  
  -- Natureza jurídica
  natureza_juridica,
  natureza_juridica.id as natureza_juridica_id,
  natureza_juridica.descricao as natureza_juridica_descricao,
  
  -- Porte da empresa
  porte,
  porte.id as porte_id,
  porte.descricao as porte_descricao,
  
  -- Matriz ou filial
  matriz_filial,
  matriz_filial.id as matriz_filial_id,
  matriz_filial.descricao as matriz_filial_descricao,
  
  -- Órgão de registro
  orgao_registro,
  orgao_registro.id as orgao_registro_id,
  orgao_registro.descricao as orgao_registro_descricao,
  
  -- Data de início das atividades
  inicio_atividade_data,
  
  -- Situação cadastral
  situacao_cadastral,
  situacao_cadastral.id as situacao_cadastral_id,
  situacao_cadastral.descricao as situacao_cadastral_descricao,
  situacao_cadastral.data as situacao_cadastral_data,
  situacao_cadastral.motivo_id as situacao_cadastral_motivo_id,
  situacao_cadastral.motivo_descricao as situacao_cadastral_motivo_descricao,
  
  -- Situação especial
  situacao_especial,
  situacao_especial.descricao as situacao_especial_descricao,
  situacao_especial.data as situacao_especial_data,
  
  -- Ente federativo
  ente_federativo,
  ente_federativo.id as ente_federativo_id,
  ente_federativo.tipo as ente_federativo_tipo,
  
  -- Informações de contato
  contato,
  contato.telefone as contato_telefone,
  contato.email as contato_email,
  
  -- Endereço completo
  endereco,
  endereco.cep as endereco_cep,
  endereco.id_pais as endereco_id_pais,
  endereco.uf as endereco_uf,
  endereco.id_municipio as endereco_id_municipio,
  endereco.municipio_nome as endereco_municipio_nome,
  endereco.municipio_exterior_nome as endereco_municipio_exterior_nome,
  endereco.bairro as endereco_bairro,
  endereco.tipo_logradouro as endereco_tipo_logradouro,
  endereco.logradouro as endereco_logradouro,
  endereco.numero as endereco_numero,
  endereco.complemento as endereco_complemento,
  
  -- Informações do contador
  contador,
  contador.pf as contador_pf,
  contador.pf.tipo_crc as contador_pf_tipo_crc,
  contador.pf.classificacao_crc as contador_pf_classificacao_crc,
  contador.pf.sequencial_crc as contador_pf_sequencial_crc,
  contador.pj as contador_pj,
  contador.pf.id as contador_pf_id,
  contador.pj.id as contador_pj_id,
  contador.pj.tipo_crc as contador_pj_tipo_crc,
  contador.pj.classificacao_crc as contador_pj_classificacao_crc,
  contador.pj.sequencial_crc as contador_pj_sequencial_crc,
  
  -- Responsável
  responsavel,
  responsavel.cpf as responsavel_cpf,
  responsavel.qualificacao_id as responsavel_qualificacao_id,
  responsavel.qualificacao_descricao as responsavel_qualificacao_descricao,
  responsavel.inclusao_data as responsavel_inclusao_data,
  
  -- Tipos e formas de atuação
  tipos_unidade,
  formas_atuacao,
  
  -- Informações dos sócios
  socios_quantidade,
  socios,
  socios.codigo_pais as socios_codigo_pais,
  socios.cpf_socio as socios_cpf_socio,
  socios.cnpj_socio as socios_cnpj_socio,
  socios.cpf_representante_legal as socios_cpf_representante_legal,
  socios.data_situacao_especial as socios_data_situacao_especial,
  socios.nome_socio_estrangeiro as socios_nome_socio_estrangeiro,
  socios.qualificacao_representante_legal as socios_qualificacao_representante_legal,
  socios.qualificacao_socio as socios_qualificacao_socio,
  socios.tipo as socios_tipo,
  
  -- Sucessões empresariais
  sucessoes,
  sucessoes.evento_sucedida as sucessoes_evento_sucedida,
  sucessoes.data_evento_sucedida as sucessoes_data_evento_sucedida,
  sucessoes.data_processamento as sucessoes_data_processamento,
  sucessoes.sucessoras as sucessoes_sucessoras,
  
  -- Metadados de extração
  timestamp,
  language,
  airbyte,
  airbyte.raw_id as airbyte_raw_id,
  airbyte.extracted_at as airbyte_extracted_at,
  airbyte.changes as airbyte_changes,
  airbyte.sync_id as airbyte_sync_id,
  airbyte.generation_id as airbyte_generation_id,
  
  -- Coluna de partição
  cnpj_particao

from {{ ref('raw_bcadastro_cnpj') }}
