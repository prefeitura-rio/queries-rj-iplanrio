{{
    config(
        alias="parte_envolvida",
        description="Dados brutos de partes envolvidas do SICOP (VW_PARTE_ENVOLVIDA_DLK)"
    )
}}

select
  safe_cast(id_processo as string)                    as id_processo,
  safe_cast(id_sequencial_parte as int64)             as id_sequencial_parte,
  safe_cast(nome_parte_envolvida as string)           as nome_parte_envolvida,
  safe_cast(endereco_parte_envolvida as string)       as endereco_parte_envolvida,
  safe_cast(numero_porta_parte_envolvida as int64)    as numero_porta_parte_envolvida,
  safe_cast(complemento_parte_envolvida as string)    as complemento_parte_envolvida,
  safe_cast(bairro_parte_envolvida as string)         as bairro_parte_envolvida,
  safe_cast(cep_parte_envolvida as int64)             as cep_parte_envolvida,
  safe_cast(telefone as string)                       as telefone,
  safe_cast(qualificacao as string)                   as qualificacao,
  safe_cast(data_procuracao as date)                  as data_procuracao,
  safe_cast(tipo_documento_parte_envolvida as string) as tipo_documento_parte_envolvida,
  safe_cast(documento_parte_envolvida as string)      as documento_parte_envolvida,
  safe_cast(matricula_responsavel as int64)           as matricula_responsavel
from {{ source("sicop","parte_envolvida") }}
