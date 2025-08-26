-- Dimensão principal de telefones RMI - Registro Mestre de Informações
-- Consolidação final de todos os telefones com qualidade e metadados
{{
    config(
        alias="int_telefone",
        schema="intermediario_rmi_telefones", 
        materialized="table",
        partition_by={"field": "data_particao", "data_type": "date"},
        cluster_by=["telefone_qualidade", "telefone_tipo", "confianca_propriedade"],
        unique_key="telefone_numero_completo"
    )
}}

with telefones_all_sources as (
  -- Usar fonte enriquecida com dados de interação
  select
    *,
    {{ classify_confianca_propriedade(
        'sistema_nome',
        'data_atualizacao',
        'ultima_resposta',
        'indicador_optout',
        'is_confirmed_by_user'
    ) }} as confianca_propriedade_source
  from {{ ref('int_telefones_com_interacao') }}
),

telefones_confianca as (
    select
        telefone_numero_completo,
        array_agg(
            struct(confianca_propriedade_source as confianca, sistema_nome, data_atualizacao)
            order by
                case
                    when confianca_propriedade_source = 'CONFIRMADA' then 1
                    when confianca_propriedade_source = 'MUITO_PROVAVEL' then 2
                    when confianca_propriedade_source = 'PROVAVEL' then 3
                    when confianca_propriedade_source = 'POUCO_PROVAVEL' then 4
                    when confianca_propriedade_source = 'IMPROVAVEL' then 5
                    else 6
                end asc,
                data_atualizacao desc
        )[offset(0)].confianca as confianca_propriedade
    from telefones_all_sources
    group by telefone_numero_completo
),

telefones_frequency as (
  -- Análise de frequência para qualidade
  select 
    telefone_numero_completo,
    count(distinct origem_id) as telefone_proprietarios_quantidade,
    count(distinct sistema_nome) as telefone_sistemas_quantidade,
    count(*) as telefone_aparicoes_quantidade
  from telefones_all_sources
  where origem_id is not null
    and regexp_contains(telefone_numero_completo, r'^[0-9]+$')
  group by telefone_numero_completo
),

telefones_aparicoes as (
  -- Estruturação das aparições
  select
    telefone_numero_completo,
    array_agg(
      struct(
        cast(sistema_nome as string) as sistema_nome,
        cast(origem_id as string) as proprietario_id,
        cast(origem_tipo as string) as proprietario_tipo,
        data_atualizacao as registro_data_atualizacao
      )
    ) as telefone_aparicoes
  from telefones_all_sources
  where origem_id is not null
    and regexp_contains(telefone_numero_completo, r'^[0-9]+$')
  group by telefone_numero_completo
),

telefones_rmi_schema as (
  select 
    freq.telefone_numero_completo,
    
    -- Decomposição do número
    {{ extract_ddi('freq.telefone_numero_completo') }} as telefone_ddi,
    {{ extract_ddd('freq.telefone_numero_completo') }} as telefone_ddd,
    {{ extract_numero('freq.telefone_numero_completo') }} as telefone_numero,
    
    -- Classificação
    {{ classify_phone_type(
        extract_ddi('freq.telefone_numero_completo'), 
        extract_ddd('freq.telefone_numero_completo'), 
        extract_numero('freq.telefone_numero_completo')
    ) }} as telefone_tipo,
    
    {{ get_nationality(extract_ddi('freq.telefone_numero_completo')) }} as telefone_nacionalidade,
    
    {{ validate_phone_quality(
        'freq.telefone_numero_completo', 
        'freq.telefone_proprietarios_quantidade'
    ) }} as telefone_qualidade,

    -- Nova coluna de confiança
    conf.confianca_propriedade,
    
    -- Metadados de aparição
    aparicoes.telefone_aparicoes,
    freq.telefone_aparicoes_quantidade,
    freq.telefone_proprietarios_quantidade,
    freq.telefone_sistemas_quantidade,
    
    -- Auditoria RMI
    current_datetime("America/Sao_Paulo") as rmi_data_atualizacao,
    '{{ var("phone_validation").rmi_version }}' as rmi_versao,
    {{ generate_hash('freq.telefone_numero_completo', 'aparicoes.telefone_aparicoes') }} as rmi_hash_validacao,

    current_date("America/Sao_Paulo") as data_particao

  from telefones_frequency freq
  left join telefones_aparicoes aparicoes using (telefone_numero_completo)
  left join telefones_confianca conf using (telefone_numero_completo)
)

select *
from telefones_rmi_schema
where telefone_numero_completo is not null