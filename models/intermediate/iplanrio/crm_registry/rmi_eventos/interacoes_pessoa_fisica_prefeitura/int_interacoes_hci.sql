{{
    config(
        alias="int_interacoes_hci",
        schema="intermediario_eventos",
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

-- Modelo intermediate para interações do sistema SMS (Saúde)
-- Transforma episódios assistenciais em interações padronizadas

with source_sms as (
    -- Seleciona e filtra os dados da fonte
    select *
    from {{ source("rj-sms", "episodio_assistencial") }}
    where paciente_cpf is not null
      and paciente_cpf != ''
      and regexp_contains(paciente_cpf, r'^\d{11}$')  -- CPF válido
      and entrada_data >= '2020-01-01'                -- Filtro temporal
      and tipo is not null                            -- Tipo obrigatório
),

interacoes_sms as (
    select
        -- IDENTIFICAÇÃO
        generate_uuid() as id_interacao,
        safe_cast(paciente_cpf as string) as cpf_cidadao,
        
        -- ORIGEM
        'HCI' as sistema_origem,
        safe_cast(id_hci as string) as protocolo_origem,
        
        -- TIPO_INTERACAO baseado no contexto de saúde
        case 
            -- CONSUMO - Atendimentos preventivos, exames, consultas de rotina
            when lower(coalesce(tipo, '')) like '%consulta%' 
                 and (lower(coalesce(subtipo, '')) like '%rotina%' 
                      or lower(coalesce(subtipo, '')) like '%preventiv%'
                      or lower(coalesce(subtipo, '')) like '%check%')
            then 'CONSUMO'
            
            when lower(coalesce(tipo, '')) like '%exame%'
                 or lower(coalesce(subtipo, '')) like '%laboratorio%'
                 or lower(coalesce(subtipo, '')) like '%raio%'
                 or lower(coalesce(subtipo, '')) like '%ultrassom%'
            then 'CONSUMO'
            
            when lower(coalesce(tipo, '')) like '%vacinação%'
                 or lower(coalesce(subtipo, '')) like '%vacina%'
            then 'CONSUMO'
            
            -- CADASTRO - Agendamentos e atividades administrativas
            when lower(coalesce(tipo, '')) like '%agendada%'
                 or lower(coalesce(subtipo, '')) like '%agendamento%'
            then 'CADASTRO'
            
            -- Default: SOLICITACAO - Atendimentos ativos (emergência, demanda espontânea, internação)
            else 'SOLICITACAO'
        end as tipo_interacao,
        
        -- CATEGORIA_INTERACAO - todos são de saúde
        'SAUDE' as categoria_interacao,
        
        -- SUBCATEGORIA baseada no tipo e subtipo específicos
        case 
            when lower(coalesce(tipo, '')) like '%vacinação%'
                 or lower(coalesce(subtipo, '')) like '%vacina%'
            then 'SAUDE_IMUNIZACAO'
            
            when lower(coalesce(tipo, '')) like '%internação%'
                 or lower(coalesce(subtipo, '')) like '%hospital%'
                 or lower(coalesce(subtipo, '')) like '%internamento%'
            then 'SAUDE_INTERNACAO'
            
            when lower(coalesce(tipo, '')) like '%exame%'
                 or lower(coalesce(subtipo, '')) like '%laboratorio%'
                 or lower(coalesce(subtipo, '')) like '%raio%'
                 or lower(coalesce(subtipo, '')) like '%ultrassom%'
                 or lower(coalesce(subtipo, '')) like '%tomografia%'
                 or lower(coalesce(subtipo, '')) like '%ressonancia%'
            then 'SAUDE_EXAMES_DIAGNOSTICOS'
            
            when lower(coalesce(subtipo, '')) like '%emergencia%'
                 or lower(coalesce(tipo, '')) like '%demanda espontanea%'
                 or lower(coalesce(subtipo, '')) like '%urgencia%'
            then 'SAUDE_ATENDIMENTO_EMERGENCIA'
            
            when lower(coalesce(subtipo, '')) like '%enfermagem%'
                 or lower(coalesce(subtipo, '')) like '%tecnico%'
            then 'SAUDE_ATENDIMENTO_ENFERMAGEM'
            
            when lower(coalesce(subtipo, '')) like '%bucal%'
                 or lower(coalesce(subtipo, '')) like '%odonto%'
                 or lower(coalesce(subtipo, '')) like '%dente%'
            then 'SAUDE_ODONTOLOGIA'
            
            when lower(coalesce(subtipo, '')) like '%domiciliar%'
                 or lower(coalesce(subtipo, '')) like '%visita%'
            then 'SAUDE_ATENDIMENTO_DOMICILIAR'
            
            when lower(coalesce(subtipo, '')) like '%ambulatorial%'
                 or lower(coalesce(subtipo, '')) like '%ambulatorio%'
            then 'SAUDE_ATENDIMENTO_AMBULATORIAL'
            
            when lower(coalesce(subtipo, '')) like '%soap%'
                 or lower(coalesce(tipo, '')) like '%consulta%'
            then 'SAUDE_CONSULTA_MEDICA'
            
            when lower(coalesce(tipo, '')) like '%agendada%'
            then 'SAUDE_AGENDAMENTO'
            
            else 'SAUDE_ATENDIMENTO_GERAL'
        end as subcategoria_interacao,
        
        -- DESCRIÇÃO utilizando subtipo quando disponível, senão tipo
        coalesce(subtipo, tipo) as descricao_interacao,
        
        -- CANAL - todos são atendimentos em unidades de saúde
        case 
            when lower(coalesce(subtipo, '')) like '%domiciliar%'
                 or lower(coalesce(subtipo, '')) like '%visita%'
            then 'ATENDIMENTO_DOMICILIAR'
            else 'UNIDADE_SAUDE'
        end as canal_interacao,
        'FISICO' as modalidade_interacao,
        
        -- TEMPORAL
        safe_cast(entrada_data as date) as data_interacao,
        safe_cast(entrada_datahora as datetime) as datahora_inicio,
        safe_cast(saida_datahora as datetime) as datahora_fim,
        safe_cast(data_particao as date) as data_particao,
        
        -- LOCALIZAÇÃO - usar dados do estabelecimento
        safe_cast(estabelecimento.id_cnes as string) as bairro_interacao,
        STRUCT(
            safe_cast(estabelecimento.nome as string) as logradouro,
            safe_cast(estabelecimento.id_cnes as string) as numero,
            safe_cast(estabelecimento.estabelecimento_tipo as string) as complemento,
            safe_cast(null as string) as bairro,
            safe_cast(null as string) as cep
        ) as endereco_interacao,
        safe_cast(null as geography) as coordenadas,  -- SMS não tem coordenadas
        
        -- RESULTADO - mapear desfecho_atendimento
        case 
            when lower(coalesce(desfecho_atendimento, '')) like '%alta%'
            then 'RESOLVIDA'
            when lower(coalesce(desfecho_atendimento, '')) like '%obito%'
                 or obito_indicador = true
            then 'NAO_APLICAVEL'  -- Casos especiais
            when desfecho_atendimento is null or trim(desfecho_atendimento) = ''
            then null
            else 'EM_ANDAMENTO'
        end as desfecho_interacao,
        
        -- DADOS FLEXÍVEIS (preservar campos específicos da saúde)
        to_json_string(struct(
            -- Campos já mapeados em colunas
            id_hci,
            paciente_cpf,
            paciente,
            tipo,
            subtipo,
            entrada_data,
            entrada_datahora,
            saida_datahora,
            motivo_atendimento,
            desfecho_atendimento,
            obito_indicador,
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            data_particao,
            
            -- Campos específicos da saúde
            exames_realizados,
            procedimentos_realizados,
            medidas,
            condicoes,
            prescricoes,
            medicamentos_administrados,
            metadados
        )) as dados_origem
        
    from source_sms
)

select * from interacoes_sms