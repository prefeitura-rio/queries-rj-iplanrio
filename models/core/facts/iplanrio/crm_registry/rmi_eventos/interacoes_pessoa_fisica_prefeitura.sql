{{
    config(
        materialized='table',
        alias='interacoes_pessoa_fisica_prefeitura',
        schema='rmi_eventos',
        partition_by={'field': 'data_particao', 'data_type': 'date'},
        cluster_by=['sistema_origem', 'tipo_interacao', 'categoria_interacao'],
        tags=['daily', 'core', 'facts']
    )
}}

-- Fact table: Interações cidadão-prefeitura unificada
-- Incluindo dados dos sistemas 1746, SMS, SMTR e Wetalkie com ontologia padronizada

with dados_1746 as (
    -- Dados do sistema 1746
    select * from {{ ref('int_interacoes_1746') }}
),

dados_sms as (
    -- Dados do sistema SMS (saúde)
    select * from {{ ref('int_interacoes_hci') }}
),

dados_smtr as (
    -- Dados do sistema SMTR (transporte público)
    select * from {{ ref('int_interacoes_bilhetagem_jae') }}
),

dados_wetalkie as (
    -- Dados do sistema Wetalkie (WhatsApp)
    select * from {{ ref('int_interacoes_wetalkie') }}
),

dados_unificados as (
    select * from dados_1746
    union all
    select * from dados_sms
    union all
    select * from dados_smtr
    union all
    select * from dados_wetalkie
),

interacoes_validadas as (
    select
        -- IDENTIFICAÇÃO (2 campos)
        coalesce(id_interacao, generate_uuid()) as id_interacao,
        cpf_cidadao,
        
        -- ORIGEM (2 campos)
        sistema_origem,
        protocolo_origem,
        
        -- CLASSIFICAÇÃO ONTOLÓGICA (4 campos - ontologia simplificada)
        tipo_interacao,
        categoria_interacao,
        subcategoria_interacao,
        descricao_interacao,
        
        -- CANAL (2 campos)
        canal_interacao,
        modalidade_interacao,
        
        -- TEMPORAL (3 campos)
        data_interacao,
        datahora_inicio,
        data_particao,
        
        -- LOCALIZAÇÃO (3 campos)
        bairro_interacao,
        endereco_interacao,
        coordenadas,
        
        -- RESULTADO (1 campo)
        desfecho_interacao,
        
        -- FLEXÍVEL (1 campo)
        coalesce(dados_origem, '{}') as dados_origem
        
    from dados_unificados
    where 
        -- Validações essenciais para dados unificados
        (
            -- Para sistemas com CPF obrigatório
            (sistema_origem in ('1746', 'HCI', 'BILHETAGEM_JAE') 
             and cpf_cidadao is not null 
             and regexp_contains(cpf_cidadao, r'^\d{11}$'))
            or
            -- Para sistemas que podem ter CPF (quando disponível via telefone) ou não
            (sistema_origem in ('WETALKIE') 
             and (cpf_cidadao is null or regexp_contains(cpf_cidadao, r'^\d{11}$')))
        )
        and data_interacao >= '2020-01-01'             -- Período válido
        and data_interacao < current_date()            -- Filtrar datas futuras (data quality)
        and datahora_inicio < current_datetime()       -- Filtrar timestamps futuros (data quality)
        and sistema_origem in ('1746', 'HCI', 'BILHETAGEM_JAE', 'WETALKIE')          -- Sistemas suportados
        and tipo_interacao in ('SOLICITACAO', 'CONSUMO', 'COMUNICACAO', 'CADASTRO', 'REPORTE')  -- Ontologia padronizada
        and categoria_interacao in ('INFRAESTRUTURA_URBANA', 'LIMPEZA_URBANA', 'FISCALIZACAO', 'MEIO_AMBIENTE', 'TRANSPORTE', 'SAUDE', 'ASSISTENCIA_SOCIAL', 'SERVICOS_PUBLICOS', 'COMUNICACAO_INSTITUCIONAL', 'OUTROS')  -- Categorias unificadas
        and modalidade_interacao in ('DIGITAL', 'FISICO')
)

select 
    *,
    -- Metadados de controle
    current_timestamp() as _datalake_loaded_at,
    '1.1' as _schema_version
from interacoes_validadas