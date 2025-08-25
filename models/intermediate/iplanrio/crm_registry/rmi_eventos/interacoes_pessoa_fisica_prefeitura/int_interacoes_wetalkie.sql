{{
    config(
        alias="int_interacoes_wetalkie",
        schema="intermediario_eventos",
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

-- Modelo intermediate para interações do sistema Wetalkie (WhatsApp)
-- Transforma disparos de comunicação em interações padronizadas

with source_wetalkie as (
    -- Seleciona e filtra os dados da fonte
    select *
    from {{ source("brutos_wetalkie_staging", "disparos_efetuados") }}
    where `to` is not null
      and `to` != ''
      and length(`to`) >= 10                 -- Telefone válido
      and safe_cast(dispatch_date as datetime) >= '2020-01-01'      -- Filtro temporal
      and campaignName is not null           -- Nome da campanha obrigatório
),

-- Buscar CPF por telefone na dimensão pessoa_fisica
-- TODO: remover referencia a tabela de prod de pessoa_fisica, indicar alguma tabela intermediaria
pessoa_fisica_phones as (
    select 
        cpf,
        concat(
            coalesce(telefone.principal.ddi, '55'),
            coalesce(telefone.principal.ddd, ''),
            telefone.principal.valor
        ) as telefone_completo
    from {{ ref("pessoa_fisica") }}
    where telefone.indicador = true 
      and telefone.principal.valor is not null
    
    union distinct
    
    -- TODO: remover referencia a tabela de prod de pessoa_fisica, indicar alguma tabela intermediaria
    select 
        cpf,
        concat(
            coalesce(alt.ddi, '55'),
            coalesce(alt.ddd, ''),
            alt.valor
        ) as telefone_completo
    from {{ ref("pessoa_fisica") }},
    unnest(telefone.alternativo) as alt
    where telefone.indicador = true 
      and alt.valor is not null
),

source_wetalkie_with_cpf as (
    select 
        w.*,
        p.cpf as cpf_encontrado
    from source_wetalkie w
    left join pessoa_fisica_phones p
        on w.`to` = p.telefone_completo
),

interacoes_wetalkie as (
    select
        -- IDENTIFICAÇÃO
        generate_uuid() as id_interacao,
        -- CPF obtido via join com dim_pessoa_fisica usando telefone
        safe_cast(cpf_encontrado as string) as cpf_cidadao,
        
        -- ORIGEM
        'WETALKIE' as sistema_origem,
        safe_cast(id_hsm as string) as protocolo_origem,
        
        -- TIPO_INTERACAO: Todas as comunicações do Wetalkie são COMUNICACAO
        -- (prefeitura comunica com cidadão via WhatsApp)
        'COMUNICACAO' as tipo_interacao,
        
        -- CATEGORIA_INTERACAO: Todas são comunicações institucionais
        'COMUNICACAO_INSTITUCIONAL' as categoria_interacao,
        
        -- SUBCATEGORIA_INTERACAO baseada no nome da campanha
        case 
            when lower(coalesce(campaignName, '')) like '%sisreg%'
                 or lower(coalesce(campaignName, '')) like '%saude%'
                 or json_extract_scalar(vars, '$.procedimento') is not null
                 or json_extract_scalar(vars, '$.unidade') is not null
            then 'COMUNICACAO_SAUDE'
            
            when lower(coalesce(campaignName, '')) like '%escola%'
                 or lower(coalesce(campaignName, '')) like '%educacao%'
                 or json_extract_scalar(vars, '$.escola') is not null
                 or json_extract_scalar(vars, '$.estudante') is not null
            then 'COMUNICACAO_EDUCACAO'
            
            when lower(coalesce(campaignName, '')) like '%teste%'
            then 'COMUNICACAO_TESTE'
            
            else 'COMUNICACAO_INSTITUCIONAL_GERAL'
        end as subcategoria_interacao,
        
        -- DESCRIÇÃO baseada no nome da campanha e conteúdo do vars
        case 
            when json_extract_scalar(vars, '$.procedimento') is not null
            then concat('Notificação de consulta: ', json_extract_scalar(vars, '$.procedimento'))
            when json_extract_scalar(vars, '$.escola') is not null
            then concat('Comunicação escolar - ', json_extract_scalar(vars, '$.escola'))
            else campaignName
        end as descricao_interacao,
        
        -- CANAL: Todas as comunicações do Wetalkie são via WhatsApp
        'WHATSAPP' as canal_interacao,
        'DIGITAL' as modalidade_interacao,
        
        -- TEMPORAL
        date(safe_cast(dispatch_date as datetime)) as data_interacao,
        safe_cast(dispatch_date as datetime) as datahora_inicio,
        safe_cast(null as datetime) as datahora_fim,  -- Não disponível
        safe_cast(data_particao as date) as data_particao,
        
        -- LOCALIZAÇÃO (geralmente não disponível para comunicações em massa)
        safe_cast(null as string) as bairro_interacao,
        STRUCT(
            safe_cast(null as string) as logradouro,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as complemento,
            safe_cast(null as string) as bairro,
            safe_cast(null as string) as cep
        ) as endereco_interacao,
        safe_cast(null as geography) as coordenadas,
        
        -- RESULTADO: Para comunicações enviadas, consideramos como resolvidas
        -- (se estão na tabela, foram enviadas com sucesso)
        'RESOLVIDA' as desfecho_interacao,
        
        -- DADOS FLEXÍVEIS (preservar todos os campos originais da comunicação)
        to_json_string(struct(
            -- Campos específicos do Wetalkie
            id_hsm,
            dispatch_date,
            campaignName,
            costCenterId,
            `to` as telefone,
            externalId,
            vars,
            ano_particao,
            mes_particao,
            data_particao,
            -- Indicador se CPF foi encontrado via telefone
            cpf_encontrado is not null as cpf_linkado_por_telefone
        )) as dados_origem
        
    from source_wetalkie_with_cpf
)

select * from interacoes_wetalkie