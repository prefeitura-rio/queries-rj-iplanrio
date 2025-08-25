{{
    config(
        alias="int_interacoes_bilhetagem_jae",
        schema="intermediario_eventos",
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

-- Modelo intermediate para interações do sistema SMTR (Secretaria de Transportes)
-- Transforma transações do transporte público em interações padronizadas

with source_smtr as (
    -- Seleciona e filtra os dados da fonte
    select *
    from {{ ref('raw_smtr__transacao_cpf') }}
    where cpf_cliente is not null
      and cpf_cliente != ''
      and regexp_contains(cpf_cliente, r'^\d{11}$')  -- CPF válido
      and data >= '2020-01-01'        -- Filtro temporal
      and datetime_transacao is not null
),

interacoes_smtr as (
    select
        -- IDENTIFICAÇÃO
        generate_uuid() as id_interacao,
        safe_cast(cpf_cliente as string) as cpf_cidadao,
        
        -- ORIGEM
        'BILHETAGEM_JAE' as sistema_origem,
        safe_cast(id_transacao as string) as protocolo_origem,
        
        -- TIPO_INTERACAO: Todas as transações de transporte são CONSUMO
        -- (cidadão consome serviço de transporte público)
        'CONSUMO' as tipo_interacao,
        
        -- CATEGORIA_INTERACAO: Todas as transações são de TRANSPORTE
        'TRANSPORTE' as categoria_interacao,
        
        -- SUBCATEGORIA_INTERACAO baseada no modo de transporte
        case 
            when lower(modo) = 'ônibus' then 'TRANSPORTE_PUBLICO_ONIBUS'
            when lower(modo) = 'brt' then 'TRANSPORTE_PUBLICO_BRT'
            when lower(modo) = 'vlt' then 'TRANSPORTE_PUBLICO_VLT'
            when lower(modo) = 'van' then 'TRANSPORTE_PUBLICO_VAN'
            when lower(modo) = 'barcas' then 'TRANSPORTE_PUBLICO_BARCAS'
            when lower(modo) = 'fretamento' then 'TRANSPORTE_PUBLICO_FRETAMENTO'
            else 'TRANSPORTE_PUBLICO_OUTROS'
        end as subcategoria_interacao,
        
        -- DESCRIÇÃO baseada no tipo de transação e modo
        concat(
            case 
                when lower(tipo_transacao_smtr) = 'gratuidade' then 'Viagem gratuita'
                when lower(tipo_transacao_smtr) = 'integral' then 'Viagem integral'
                when lower(tipo_transacao_smtr) = 'integração' then 'Viagem com integração'
                when lower(tipo_transacao_smtr) = 'transferência' then 'Transferência'
                else 'Viagem'
            end,
            ' - ',
            modo,
            case 
                when operadora is not null then concat(' (', operadora, ')')
                else ''
            end,
            case 
                when servico_jae is not null then concat(' - Linha ', servico_jae)
                else ''
            end
        ) as descricao_interacao,
        
        -- CANAL: Transações de transporte são sempre através do validador físico
        case 
            when modo in ('Ônibus', 'BRT', 'Van', 'Fretamento') then 'VALIDADOR_ONIBUS'
            when modo = 'VLT' then 'VALIDADOR_VLT'
            when modo = 'Barcas' then 'VALIDADOR_BARCAS'
            else 'VALIDADOR_TRANSPORTE'
        end as canal_interacao,
        
        -- MODALIDADE: Sempre físico (pessoa presente no veículo)
        'FISICO' as modalidade_interacao,
        
        -- TEMPORAL
        safe_cast(data as date) as data_interacao,
        safe_cast(datetime_transacao as datetime) as datahora_inicio,
        safe_cast(datetime_transacao as datetime) as datahora_fim, -- Para transporte, início = fim
        safe_cast(data as date) as data_particao,
        
        -- LOCALIZAÇÃO
        safe_cast(null as string) as bairro_interacao, -- Não temos bairro nas transações
        STRUCT(
            safe_cast(null as string) as logradouro,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as complemento,
            safe_cast(null as string) as bairro,
            safe_cast(null as string) as cep
        ) as endereco_interacao,
        -- Coordenadas da transação quando disponíveis
        case 
            when latitude is not null and longitude is not null 
            then st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64))
            else null
        end as coordenadas,
        
        -- RESULTADO: Transações de transporte são sempre bem-sucedidas (se estão na base)
        'RESOLVIDA' as desfecho_interacao,
        
        -- DADOS FLEXÍVEIS (preservar todos os campos originais disponíveis)
        to_json_string(struct(
            -- Campos técnicos do transporte
            hora,
            datetime_processamento,
            datetime_captura,
            modo,
            id_consorcio,
            consorcio,
            id_operadora,
            operadora,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            tipo_pagamento,
            tipo_transacao,
            tipo_transacao_smtr,
            tipo_gratuidade,
            geo_point_transacao,
            valor_transacao,
            versao,
            datetime_ultima_atualizacao
        )) as dados_origem
        
    from source_smtr
)

select * from interacoes_smtr