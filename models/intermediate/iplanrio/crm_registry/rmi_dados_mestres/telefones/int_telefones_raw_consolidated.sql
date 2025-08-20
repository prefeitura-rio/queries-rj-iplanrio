-- Consolida todos os telefones de todas as fontes do sistema
-- Padroniza formato e extrai informações básicas para análise RMI
{{
    config(
        alias="telefones_raw_consolidated",
        schema="intermediario_rmi_telefones",
        materialized=("table" if target.name == "dev" else "ephemeral"),
    )
}}

with 

-- PESSOA FÍSICA - BCadastro (Receita Federal)
-- TYPES: origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
-- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME
telefones_bcadastro_cpf as (
  select 
    t.cpf as origem_id,  -- Use table alias to be explicit
    'CPF' as origem_tipo,
    -- Use DDI from data, don't assume Brazilian numbers
    concat(
      coalesce(t.contato.telefone.ddi, '55'), 
      t.contato.telefone.ddd, 
      {{ padronize_telefone_rmi('t.contato.telefone.numero') }}
    ) as telefone_numero_completo,
    'bcadastro' as sistema_nome,
    'bcadastro_cpf.contato.telefone' as campo_origem,
    'PESSOAL' as contexto,
    atualizacao_data as data_atualizacao
  from {{ source('brutos_bcadastro', 'cpf') }} as t
  where t.contato.telefone.numero is not null
),

-- PESSOA JURÍDICA - BCadastro (Receita Federal)  
-- TYPES: origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
-- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME
telefones_bcadastro_cnpj as (
  select 
    c.cnpj as origem_id,  -- Use table alias to be explicit
    'CNPJ' as origem_tipo,
    -- BCadastro CNPJ has telefone array with {ddd, telefone} structure
    concat('55', tel.ddd, {{ padronize_telefone_rmi('tel.telefone') }}) as telefone_numero_completo,
    'bcadastro' as sistema_nome,
    'bcadastro_cnpj.contato.telefone' as campo_origem,
    'EMPRESARIAL' as contexto,
    cast(null as date) as data_atualizacao
  from {{ source('brutos_bcadastro', 'cnpj') }} as c,
    unnest(c.contato.telefone) as tel
  where tel.telefone is not null
),

sms_data_atualizacao as (
  SELECT cpf, tel.telefone_raw, date(data_ultima_atualizacao_cadastral) as data_atualizacao
  FROM  {{ source('rj-sms-projeto-whatsapp', 'telefones_validos') }},
       unnest(telefones) as tel
),

-- SAÚDE - Registros SMS
-- TYPES: origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
-- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME (cast from TIMESTAMP)
telefones_sms_cns as (
  select 
    cns_item as origem_id,  -- Already string from unnest
    'CNS' as origem_tipo,
    -- SMS has telefone array with {ddd, valor, sistema, rank} structure
    concat('55', tel.ddd, {{ padronize_telefone_rmi('tel.valor') }}) as telefone_numero_completo,
    'sms' as sistema_nome,
    'sms_paciente.contato.telefone' as campo_origem,
    'SAUDE' as contexto,
     data_atualizacao
  from {{ source('rj-sms', 'paciente') }} as s,
    unnest(s.cns) as cns_item,
    unnest(s.contato.telefone) as tel
  left join sms_data_atualizacao validos
         on validos.cpf = s.cpf and tel.valor = validos.telefone_raw
  where tel.valor is not null and cns_item is not null
),

-- telefones_sms_cns_data as (
--   select telefones_sms_cns.* EXCEPT(data_atualizacao),
--   date(telefones.data_ultima_atualizacao_cadastral) as data_atualizacao
--   from telefones_sms_cns
--   left join sms_data_atualizacao validos
--          on validos.cpf = telefones_sms_cns.origem_id and validos.telefone_raw = validos.telefone_raw
-- )
telefones_sms_cpf as (
  select 
    s.cpf as origem_id, 
    'CPF' as origem_tipo,
    -- SMS has telefone array with {ddd, valor, sistema, rank} structure
    concat('55', tel.ddd, {{ padronize_telefone_rmi('tel.valor') }}) as telefone_numero_completo,
    'sms' as sistema_nome,
    'sms_paciente.contato.telefone' as campo_origem,
    'SAUDE' as contexto,
    data_atualizacao
  from {{ source('rj-sms', 'paciente') }} as s,
    unnest(s.contato.telefone) as tel
  left join sms_data_atualizacao validos
         on validos.cpf = s.cpf and tel.valor = validos.telefone_raw
  where tel.valor is not null and s.cpf is not null
),

-- FUNCIONAL - ERGON (servidores públicos) - Celular
-- TYPES: origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
-- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME
telefones_ergon_celular as (
  select 
    lpad(e.id_cpf, 11, '0') as origem_id,  -- Standardize CPF format with leading zeros
    'CPF' as origem_tipo,
    -- ERGON celular numbers need Brazilian DDI added and cleaning
    concat('55', {{ padronize_telefone_rmi('e.celular') }}) as telefone_numero_completo,
    'ergon' as sistema_nome,
    'ergon_funcionario.celular' as campo_origem,
    'FUNCIONAL' as contexto,
    date(updated_at) as data_atualizacao
  from {{ source('rj-smfp', 'funcionario') }} as e
  where e.celular is not null 
    and e.id_cpf is not null
    and lpad(e.id_cpf, 11, '0') != '00000000000'  -- Exclude invalid CPFs
),

-- FUNCIONAL - ERGON (servidores públicos) - Telefone Fixo
-- TYPES: origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
-- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME
telefones_ergon_telefone as (
  select 
    lpad(e.id_cpf, 11, '0') as origem_id,  -- Standardize CPF format with leading zeros
    'CPF' as origem_tipo,
    -- ERGON telefone numbers need Brazilian DDI added and cleaning
    concat('55', {{ padronize_telefone_rmi('e.telefone') }}) as telefone_numero_completo,
    'ergon' as sistema_nome,
    'ergon_funcionario.telefone' as campo_origem,
    'FUNCIONAL' as contexto,
    date(updated_at) as data_atualizacao
  from {{ source('rj-smfp', 'funcionario') }} as e
  where e.telefone is not null 
    and e.id_cpf is not null
    and lpad(e.id_cpf, 11, '0') != '00000000000'  -- Exclude invalid CPFs
),

telefones_all_sources as (
  -- BCadastro CPF (Pessoas Físicas)
  select * from telefones_bcadastro_cpf
  
  union all
  
  -- BCadastro CNPJ (Pessoas Jurídicas)
  select * from telefones_bcadastro_cnpj
  
  union all
  
  -- SMS Saúde (Pacientes do sistema de saúde)
  select * from telefones_sms_cns

  union all

  select * from telefones_sms_cpf

  union all
  
  -- ERGON Celular (Funcionários públicos - celular)
  select * from telefones_ergon_celular

  union all
  
  -- ERGON Telefone (Funcionários públicos - telefone fixo)
  select * from telefones_ergon_telefone

  
  -- All sources now use explicit table aliases and consistent field types:
  -- origem_id STRING, origem_tipo STRING, telefone_numero_completo STRING, 
  -- sistema_nome STRING, campo_origem STRING, contexto STRING, data_atualizacao DATETIME
)

select 
  origem_id,
  origem_tipo,
  telefone_numero_completo,
  sistema_nome,
  campo_origem,
  contexto,
  data_atualizacao
from telefones_all_sources
where telefone_numero_completo is not null
  and length(telefone_numero_completo) >= 10