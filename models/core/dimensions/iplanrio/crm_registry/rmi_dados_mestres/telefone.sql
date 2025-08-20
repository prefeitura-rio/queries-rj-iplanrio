-- Dimensão principal de telefones RMI - Registro Mestre de Informações
-- Consolidação final de todos os telefones com qualidade e metadados
{{
    config(
        alias="telefone",
        schema="rmi_dados_mestres", 
        materialized="table",
        partition_by={"field": "rmi_data_criacao", "data_type": "datetime"},
        cluster_by=["telefone_qualidade", "telefone_tipo", "confianca_propriedade"],
        unique_key="telefone_numero_completo"
    )
}}

  -- Usar fonte enriquecida com dados de interação
  select
    *
  from {{ ref('int_telefones') }}
