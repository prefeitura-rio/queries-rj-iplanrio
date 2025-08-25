{{
    config(
        alias="int_interacoes_1746",
        schema="intermediario_eventos",
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

-- Modelo intermediate para interações do sistema 1746 (SEGOVI)
-- Transforma chamados do 1746 em interações padronizadas

with source_1746 as (
    -- Seleciona e filtra os dados da fonte
    select *
    from {{ source("rj-segovi", "1746_chamado_cpf") }}
    where cpf is not null
      and cpf != ''
      and regexp_contains(cpf, r'^\d{11}$')  -- CPF válido
      and data_inicio >= '2020-01-01'        -- Filtro temporal
),

interacoes_1746 as (
    select
        -- IDENTIFICAÇÃO
        generate_uuid() as id_interacao,
        safe_cast(cpf as string) as cpf_cidadao,
        
        -- ORIGEM
        '1746' as sistema_origem,
        safe_cast(id_chamado as string) as protocolo_origem,
        
        -- TIPO_INTERACAO simplificado (5 tipos principais)
        case 
            -- CONSUMO - Informações, consultas, orientações (cidadão consome informação)
            when lower(coalesce(tipo, '')) like '%processos%'
                 or lower(coalesce(tipo, '')) like '%qualidade do atendimento%'
                 or lower(coalesce(tipo, '')) like '%atendimento ao cidadão%'
                 or lower(coalesce(subtipo, '')) like '%orientação%'
                 or lower(coalesce(subtipo, '')) like '%informação%'
                 or lower(coalesce(subtipo, '')) like '%consulta%'
                 or lower(coalesce(subtipo, '')) like '%número do processo%'
                 or lower(coalesce(subtipo, '')) like '%pela internet%'
                 or lower(coalesce(subtipo, '')) like '%correção de falhas%'
            then 'CONSUMO'
            
            -- COMUNICACAO - Campanhas, notificações, mensagens institucionais
            when lower(coalesce(tipo, '')) like '%campanha%'
                 or lower(coalesce(tipo, '')) like '%comunicação%'
                 or lower(coalesce(tipo, '')) like '%notificação%'
                 or lower(coalesce(subtipo, '')) like '%campanha%'
                 or lower(coalesce(subtipo, '')) like '%comunicado%'
            then 'COMUNICACAO'
            
            -- CADASTRO - Atualizações, correções, renovações cadastrais
            when lower(coalesce(tipo, '')) like '%cadastro%'
                 or lower(coalesce(tipo, '')) like '%atualização%'
                 or lower(coalesce(tipo, '')) like '%renovação%'
                 or lower(coalesce(subtipo, '')) like '%cadastro%'
                 or lower(coalesce(subtipo, '')) like '%atualização%'
                 or lower(coalesce(subtipo, '')) like '%dados pessoais%'
            then 'CADASTRO'
            
            -- Default: SOLICITACAO - Todos os pedidos ativos (serviços, fiscalização, reparos)
            -- Inclui tanto pedidos de serviços quanto reports de problemas e fiscalização
            else 'SOLICITACAO'
        end as tipo_interacao,
        
        -- CATEGORIA_INTERACAO baseada no domínio do serviço
        case 
            -- INFRAESTRUTURA_URBANA
            when lower(coalesce(tipo, '')) like '%iluminação%'
                 or lower(coalesce(tipo, '')) like '%pavimentação%' 
                 or lower(coalesce(tipo, '')) like '%drenagem%'
                 or lower(coalesce(tipo, '')) like '%sinal%'
                 or lower(coalesce(tipo, '')) like '%semáforo%'
                 or lower(coalesce(tipo, '')) like '%mobiliário urbano%'
                 or lower(coalesce(subtipo, '')) like '%lâmpada%'
                 or lower(coalesce(subtipo, '')) like '%luminária%'
                 or lower(coalesce(subtipo, '')) like '%buraco%'
                 or lower(coalesce(subtipo, '')) like '%pista%'
                 or lower(coalesce(subtipo, '')) like '%bueiro%'
                 or lower(coalesce(subtipo, '')) like '%saneamento%'
                 or lower(coalesce(subtipo, '')) like '%trânsito%'
                 or lower(coalesce(subtipo, '')) like '%semáforo%'
                 or lower(coalesce(subtipo, '')) like '%placa com nome%'
            then 'INFRAESTRUTURA_URBANA'
            
            -- LIMPEZA_URBANA
            when lower(coalesce(tipo, '')) like '%remoção gratuita%'
                 or lower(coalesce(tipo, '')) like '%limpeza%'
                 or lower(coalesce(tipo, '')) like '%coleta%'
                 or lower(coalesce(tipo, '')) like '%papeleiras%'
                 or lower(coalesce(tipo, '')) like '%instalação e manutenção de contêineres%'
                 or lower(coalesce(subtipo, '')) like '%entulho%'
                 or lower(coalesce(subtipo, '')) like '%bens inservíveis%'
                 or lower(coalesce(subtipo, '')) like '%varrição%'
                 or lower(coalesce(subtipo, '')) like '%capina%'
                 or lower(coalesce(subtipo, '')) like '%logradouro%'
                 or lower(coalesce(subtipo, '')) like '%coleta%'
                 or lower(coalesce(subtipo, '')) like '%papeleira%'
                 or lower(coalesce(subtipo, '')) like '%contêiner%'
            then 'LIMPEZA_URBANA'
            
            -- FISCALIZACAO
            when lower(coalesce(tipo, '')) like '%fiscalização%'
                 or lower(coalesce(tipo, '')) like '%estacionamento%'
                 or lower(coalesce(tipo, '')) like '%estrutura%'
                 or lower(coalesce(tipo, '')) like '%perturbação do sossego%'
                 or lower(coalesce(tipo, '')) like '%poluição sonora%'
                 or lower(coalesce(tipo, '')) like '%ocupação de área pública%'
                 or lower(coalesce(tipo, '')) like '%comércio ambulante%'
                 or lower(coalesce(tipo, '')) like '%veículo%'
                 or lower(coalesce(tipo, '')) like '%patrulhamento%'
                 or lower(coalesce(tipo, '')) like '%vias públicas%'
                 or lower(coalesce(tipo, '')) like '%postura municipal%'
            then 'FISCALIZACAO'
            
            -- MEIO_AMBIENTE
            when lower(coalesce(tipo, '')) like '%arbóreo%'
                 or lower(coalesce(tipo, '')) like '%vetor%'
                 or lower(coalesce(tipo, '')) like '%proteção de animais%'
                 or lower(coalesce(tipo, '')) like '%danos ao meio ambiente%'
                 or lower(coalesce(tipo, '')) like '%poluição%'
                 or lower(coalesce(tipo, '')) like '%parques%'
                 or lower(coalesce(tipo, '')) like '%praças%'
                 or lower(coalesce(subtipo, '')) like '%árvore%'
                 or lower(coalesce(subtipo, '')) like '%poda%'
                 or lower(coalesce(subtipo, '')) like '%roedor%'
                 or lower(coalesce(subtipo, '')) like '%aedes%'
                 or lower(coalesce(subtipo, '')) like '%caramujo%'
                 or lower(coalesce(subtipo, '')) like '%maus tratos%'
                 or lower(coalesce(subtipo, '')) like '%resgate de animais%'
            then 'MEIO_AMBIENTE'
            
            -- TRANSPORTE
            when lower(coalesce(tipo, '')) like '%ônibus%'
                 or lower(coalesce(tipo, '')) like '%táxi%'
                 or lower(coalesce(tipo, '')) like '%transporte%'
                 or lower(coalesce(tipo, '')) like '%regulamentações viárias%'
                 or lower(coalesce(tipo, '')) like '%ciclovias%'
                 or lower(coalesce(subtipo, '')) like '%ônibus%'
            then 'TRANSPORTE'
            
            -- SAUDE
            when lower(coalesce(tipo, '')) like '%programas de saúde%'
                 or lower(coalesce(tipo, '')) like '%programa cegonha%'
                 or lower(coalesce(tipo, '')) like '%zoonoses%'
                 or lower(coalesce(tipo, '')) like '%atendimento - coronavírus%'
                 or lower(coalesce(tipo, '')) like '%visa%'
                 or lower(coalesce(tipo, '')) like '%vigilância sanitária%'
                 or lower(coalesce(tipo, '')) like '%engenharia sanitária%'
                 or lower(coalesce(tipo, '')) like '%estabelecimentos e serviços de saúde%'
                 or lower(coalesce(subtipo, '')) like '%gestante%'
                 or lower(coalesce(subtipo, '')) like '%maternidade%'
                 or lower(coalesce(subtipo, '')) like '%raiva animal%'
                 or lower(coalesce(subtipo, '')) like '%coronavirus%'
            then 'SAUDE'
            
            -- ASSISTENCIA_SOCIAL
            when lower(coalesce(tipo, '')) like '%atendimento social%'
                 or lower(coalesce(tipo, '')) like '%conselho tutelar%'
                 or lower(coalesce(tipo, '')) like '%auxílio à população%'
                 or lower(coalesce(tipo, '')) like '%direitos humanos%'
                 or lower(coalesce(subtipo, '')) like '%cras%'
                 or lower(coalesce(subtipo, '')) like '%creas%'
                 or lower(coalesce(subtipo, '')) like '%situação de rua%'
            then 'ASSISTENCIA_SOCIAL'
            
            -- SERVICOS_PUBLICOS
            when lower(coalesce(tipo, '')) like '%alvará%'
                 or lower(coalesce(tipo, '')) like '%documento%'
                 or lower(coalesce(tipo, '')) like '%cópia de projetos%'
                 or lower(coalesce(tipo, '')) like '%atendimento ao cidadão%'
                 or lower(coalesce(tipo, '')) like '%processos%'
                 or lower(coalesce(tipo, '')) like '%qualidade do atendimento%'
                 or lower(coalesce(tipo, '')) like '%defesa do consumidor%'
                 or lower(coalesce(tipo, '')) like '%geotecnia%'
                 or lower(coalesce(tipo, '')) like '%obras%'
                 or lower(coalesce(tipo, '')) like '%cemitérios%'
                 or lower(coalesce(tipo, '')) like '%educação%'
                 or lower(coalesce(tipo, '')) like '%cultura%'
                 or lower(coalesce(tipo, '')) like '%trabalho%'
                 or lower(coalesce(tipo, '')) like '%aplicativos%'
            then 'SERVICOS_PUBLICOS'
            
            else 'OUTROS'
        end as categoria_interacao,
        
        -- SUBCATEGORIA baseada no mapeamento do documento de análise
        case 
            when lower(coalesce(tipo, '')) like '%remoção gratuita%' 
                 or lower(coalesce(subtipo, '')) like '%entulho%'
                 or lower(coalesce(subtipo, '')) like '%bens inservíveis%'
            then 'LIMPEZA_URBANA_REMOCAO_ENTULHO'
            
            when lower(coalesce(tipo, '')) like '%iluminação%' 
                 or lower(coalesce(subtipo, '')) like '%lâmpada%'
                 or lower(coalesce(subtipo, '')) like '%luminária%'
            then 'INFRAESTRUTURA_ILUMINACAO_PUBLICA'
            
            when lower(coalesce(tipo, '')) like '%estacionamento%'
                 or lower(coalesce(subtipo, '')) like '%estacionamento irregular%'
            then 'FISCALIZACAO_TRANSITO_ESTACIONAMENTO'
            
            when lower(coalesce(tipo, '')) like '%limpeza%'
                 or lower(coalesce(subtipo, '')) like '%varrição%'
                 or lower(coalesce(subtipo, '')) like '%capina%'
                 or lower(coalesce(subtipo, '')) like '%logradouro%'
                 or lower(coalesce(tipo, '')) like '%coleta domiciliar%'
                 or lower(coalesce(subtipo, '')) like '%coleta domiciliar%'
            then 'LIMPEZA_URBANA_LOGRADOUROS'
            
            when lower(coalesce(tipo, '')) like '%pavimentação%'
                 or lower(coalesce(subtipo, '')) like '%buraco%'
                 or lower(coalesce(subtipo, '')) like '%pista%'
            then 'INFRAESTRUTURA_PAVIMENTACAO'
            
            when lower(coalesce(tipo, '')) like '%arbóreo%'
                 or lower(coalesce(subtipo, '')) like '%árvore%'
                 or lower(coalesce(subtipo, '')) like '%poda%'
            then 'MEIO_AMBIENTE_MANEJO_ARVORES'
            
            when lower(coalesce(tipo, '')) like '%drenagem%'
                 or lower(coalesce(subtipo, '')) like '%bueiro%'
                 or lower(coalesce(subtipo, '')) like '%saneamento%'
            then 'INFRAESTRUTURA_DRENAGEM'
            
            when lower(coalesce(tipo, '')) like '%vetor%'
                 or lower(coalesce(subtipo, '')) like '%roedor%'
                 or lower(coalesce(subtipo, '')) like '%aedes%'
                 or lower(coalesce(subtipo, '')) like '%caramujo%'
            then 'MEIO_AMBIENTE_CONTROLE_VETORES'
            
            when lower(coalesce(tipo, '')) like '%estrutura%'
                 or lower(coalesce(subtipo, '')) like '%imóvel%'
                 or lower(coalesce(subtipo, '')) like '%obra%'
            then 'FISCALIZACAO_ESTRUTURA_IMOVEL'
            
            when lower(coalesce(tipo, '')) like '%sinal%'
                 or lower(coalesce(subtipo, '')) like '%trânsito%'
                 or lower(coalesce(subtipo, '')) like '%semáforo%'
            then 'INFRAESTRUTURA_SINALIZACAO'
            
            -- Novas categorias para reduzir OUTROS
            when lower(coalesce(tipo, '')) like '%perturbação do sossego%'
                 or lower(coalesce(tipo, '')) like '%poluição sonora%'
                 or lower(coalesce(subtipo, '')) like '%perturbação%'
                 or lower(coalesce(subtipo, '')) like '%sossego%'
            then 'FISCALIZACAO_POLUICAO_SONORA'
            
            when lower(coalesce(tipo, '')) like '%atendimento social%'
                 or lower(coalesce(subtipo, '')) like '%cras%'
                 or lower(coalesce(subtipo, '')) like '%creas%'
                 or lower(coalesce(subtipo, '')) like '%situação de rua%'
            then 'ASSISTENCIA_SOCIAL_ATENDIMENTO'
            
            when lower(coalesce(tipo, '')) like '%ônibus%'
                 or lower(coalesce(subtipo, '')) like '%ônibus%'
                 or lower(coalesce(subtipo, '')) like '%ar condicionado%'
                 or lower(coalesce(subtipo, '')) like '%intervalo irregular%'
            then 'TRANSPORTE_PUBLICO_ONIBUS'
            
            when lower(coalesce(tipo, '')) like '%proteção de animais%'
                 or lower(coalesce(tipo, '')) like '%danos ao meio ambiente%'
                 or lower(coalesce(subtipo, '')) like '%maus tratos%'
                 or lower(coalesce(subtipo, '')) like '%resgate de animais%'
            then 'MEIO_AMBIENTE_PROTECAO_ANIMAIS'
            
            when lower(coalesce(tipo, '')) like '%programas de saúde%'
                 or lower(coalesce(tipo, '')) like '%programa cegonha%'
                 or lower(coalesce(subtipo, '')) like '%gestante%'
                 or lower(coalesce(subtipo, '')) like '%maternidade%'
            then 'SAUDE_PROGRAMAS_ESPECIAIS'
            
            when lower(coalesce(tipo, '')) like '%alvará%'
                 or lower(coalesce(subtipo, '')) like '%alvará%'
                 or lower(coalesce(subtipo, '')) like '%atividades econômicas%'
            then 'LICENCIAMENTO_ALVARA'
            
            when lower(coalesce(tipo, '')) like '%comércio ambulante%'
                 or lower(coalesce(subtipo, '')) like '%ambulante%'
            then 'FISCALIZACAO_COMERCIO_AMBULANTE'
            
            when lower(coalesce(tipo, '')) like '%veículo%'
                 or lower(coalesce(subtipo, '')) like '%veículo abandonado%'
                 or lower(coalesce(subtipo, '')) like '%remoção de veículo%'
            then 'FISCALIZACAO_VEICULO_ABANDONADO'
            
            when lower(coalesce(tipo, '')) like '%documento - defesa civil%'
                 or lower(coalesce(tipo, '')) like '%cópia de projetos%'
                 or lower(coalesce(subtipo, '')) like '%vistoria da defesa civil%'
                 or lower(coalesce(subtipo, '')) like '%planta baixa%'
            then 'SERVICOS_DOCUMENTOS_TECNICOS'
            
            when lower(coalesce(tipo, '')) like '%patrulhamento%'
                 or lower(coalesce(tipo, '')) like '%vias públicas%'
                 or lower(coalesce(subtipo, '')) like '%calçada%'
                 or lower(coalesce(subtipo, '')) like '%obstáculo%'
                 or lower(coalesce(subtipo, '')) like '%obstruída%'
            then 'FISCALIZACAO_VIAS_PUBLICAS'
            
            when lower(coalesce(tipo, '')) like '%atendimento ao cidadão%'
                 or lower(coalesce(subtipo, '')) like '%portal%'
                 or lower(coalesce(subtipo, '')) like '%app 1746%'
                 or lower(coalesce(subtipo, '')) like '%cadastro%'
            then 'SERVICOS_ATENDIMENTO_CIDADAO'
            
            -- Categorias adicionais para reduzir OUTROS para <1%
            when lower(coalesce(tipo, '')) like '%zoonoses%'
                 or lower(coalesce(subtipo, '')) like '%cães%'
                 or lower(coalesce(subtipo, '')) like '%gatos%'
                 or lower(coalesce(subtipo, '')) like '%raiva animal%'
                 or lower(coalesce(subtipo, '')) like '%cavalos%'
                 or lower(coalesce(subtipo, '')) like '%bois%'
                 or lower(coalesce(subtipo, '')) like '%porcos%'
                 or lower(coalesce(subtipo, '')) like '%cabras%'
            then 'SAUDE_ZOONOSES_CONTROLE_ANIMAIS'
            
            when lower(coalesce(tipo, '')) like '%instalação e manutenção de contêineres%'
                 or lower(coalesce(tipo, '')) like '%papeleiras%'
                 or lower(coalesce(subtipo, '')) like '%papeleira%'
                 or lower(coalesce(subtipo, '')) like '%contêiner%'
                 or lower(coalesce(subtipo, '')) like '%caçamba da comlurb%'
            then 'LIMPEZA_URBANA_EQUIPAMENTOS'
            
            when lower(coalesce(tipo, '')) like '%poluição%'
                 or lower(coalesce(subtipo, '')) like '%poluição do ar%'
                 or lower(coalesce(subtipo, '')) like '%medição de ruído%'
            then 'MEIO_AMBIENTE_CONTROLE_POLUICAO'
            
            when lower(coalesce(tipo, '')) like '%defesa do consumidor%'
                 or lower(coalesce(subtipo, '')) like '%produtos ou serviços%'
                 or lower(coalesce(subtipo, '')) like '%fornecedores%'
            then 'DIREITOS_CONSUMIDOR'
            
            when lower(coalesce(tipo, '')) like '%ocupação de área pública%'
                 or lower(coalesce(subtipo, '')) like '%mesas e cadeiras%'
                 or lower(coalesce(subtipo, '')) like '%ocupação%'
            then 'FISCALIZACAO_OCUPACAO_AREA_PUBLICA'
            
            when lower(coalesce(tipo, '')) like '%fiscalização de caçamba%'
                 or lower(coalesce(subtipo, '')) like '%limpeza de resíduos em terreno%'
                 or lower(coalesce(subtipo, '')) like '%terreno baldio%'
            then 'FISCALIZACAO_LIMPEZA_TERRENOS'
            
            when lower(coalesce(tipo, '')) like '%visa alimentos%'
                 or lower(coalesce(tipo, '')) like '%alimentos e estabelecimentos%'
                 or lower(coalesce(tipo, '')) like '%mercados, supermercados%'
                 or lower(coalesce(tipo, '')) like '%visa   saúde%'
                 or lower(coalesce(tipo, '')) like '%vigilância sanitária%'
            then 'VIGILANCIA_SANITARIA'
            
            when lower(coalesce(tipo, '')) like '%atendimento - coronavírus%'
                 or lower(coalesce(subtipo, '')) like '%aglomeração%'
                 or lower(coalesce(subtipo, '')) like '%coronavirus%'
            then 'SAUDE_EMERGENCIA_SANITARIA'
            
            when lower(coalesce(tipo, '')) like '%geotecnia%'
                 or lower(coalesce(subtipo, '')) like '%deslizamento%'
                 or lower(coalesce(subtipo, '')) like '%barreira%'
                 or lower(coalesce(subtipo, '')) like '%encosta%'
                 or lower(coalesce(subtipo, '')) like '%talude%'
            then 'DEFESA_CIVIL_GEOTECNIA'
            
            when lower(coalesce(tipo, '')) like '%táxi%'
                 or lower(coalesce(tipo, '')) like '%táxi.rio%'
            then 'TRANSPORTE_TAXI'
            
            when lower(coalesce(tipo, '')) like '%processos%'
                 or lower(coalesce(subtipo, '')) like '%número do processo%'
                 or lower(coalesce(subtipo, '')) like '%transportes%'
            then 'SERVICOS_PROCESSOS_ADMINISTRATIVOS'
            
            when lower(coalesce(tipo, '')) like '%mobiliário urbano%'
                 or lower(coalesce(subtipo, '')) like '%placa com nome de rua%'
                 or lower(coalesce(subtipo, '')) like '%instalação%'
                 or lower(coalesce(subtipo, '')) like '%remoção%'
                 or lower(coalesce(subtipo, '')) like '%manutenção%'
            then 'INFRAESTRUTURA_MOBILIARIO_URBANO'
            
            when lower(coalesce(tipo, '')) like '%coleta seletiva%'
            then 'LIMPEZA_URBANA_COLETA_SELETIVA'
            
            when lower(coalesce(tipo, '')) like '%regulamentações viárias%'
                 or lower(coalesce(tipo, '')) like '%engenharia de tráfego%'
            then 'TRANSPORTE_REGULAMENTACAO_VIARIA'
            
            when lower(coalesce(tipo, '')) like '%parques urbanos%'
                 or lower(coalesce(tipo, '')) like '%praças%'
                 or lower(coalesce(tipo, '')) like '%parques%'
                 or lower(coalesce(tipo, '')) like '%jardins%'
            then 'MEIO_AMBIENTE_PARQUES_PRACAS'
            
            when lower(coalesce(tipo, '')) like '%transporte especial%'
                 or lower(coalesce(tipo, '')) like '%tec%'
                 or lower(coalesce(tipo, '')) like '%gratuidade de transporte%'
            then 'TRANSPORTE_ESPECIAL'
            
            when lower(coalesce(tipo, '')) like '%qualidade do atendimento%'
                 or lower(coalesce(tipo, '')) like '%central 1746%'
            then 'SERVICOS_QUALIDADE_ATENDIMENTO'
            
            when lower(coalesce(tipo, '')) like '%postura municipal%'
                 or lower(coalesce(tipo, '')) like '%fiscalização eletrônica%'
                 or lower(coalesce(tipo, '')) like '%fiscalização de grande gerador%'
            then 'FISCALIZACAO_POSTURA_MUNICIPAL'
            
            when lower(coalesce(tipo, '')) like '%atividades esportivas%'
                 or lower(coalesce(tipo, '')) like '%programação cultural%'
                 or lower(coalesce(tipo, '')) like '%equipamentos culturais%'
                 or lower(coalesce(tipo, '')) like '%carnaval%'
            then 'CULTURA_ESPORTE_LAZER'
            
            when lower(coalesce(tipo, '')) like '%educação especial%'
                 or lower(coalesce(tipo, '')) like '%escola%'
                 or lower(coalesce(tipo, '')) like '%multirio%'
            then 'EDUCACAO'
            
            when lower(coalesce(tipo, '')) like '%solicitação de obras%'
                 or lower(coalesce(tipo, '')) like '%fiscalização de obras%'
                 or lower(coalesce(tipo, '')) like '%licença de obras%'
                 or lower(coalesce(tipo, '')) like '%conservação de vias%'
            then 'OBRAS_PUBLICAS'
            
            when lower(coalesce(tipo, '')) like '%cemitérios%'
                 or lower(coalesce(tipo, '')) like '%serviços funerários%'
            then 'SERVICOS_FUNERARIOS'
            
            when lower(coalesce(tipo, '')) like '%publicidade%'
                 or lower(coalesce(tipo, '')) like '%monumentos%'
                 or lower(coalesce(tipo, '')) like '%chafarizes%'
            then 'PATRIMONIO_PUBLICIDADE'
            
            when lower(coalesce(tipo, '')) like '%engenharia sanitária%'
                 or lower(coalesce(tipo, '')) like '%estabelecimentos e serviços de saúde%'
            then 'SAUDE_INFRAESTRUTURA'
            
            when lower(coalesce(tipo, '')) like '%conselho tutelar%'
                 or lower(coalesce(tipo, '')) like '%auxílio à população%'
                 or lower(coalesce(tipo, '')) like '%racismo%'
                 or lower(coalesce(tipo, '')) like '%intolerância religiosa%'
                 or lower(coalesce(tipo, '')) like '%antissemitismo%'
                 or lower(coalesce(tipo, '')) like '%assédio%'
            then 'DIREITOS_HUMANOS_ASSISTENCIA'
            
            when lower(coalesce(tipo, '')) like '%ciclovias%'
                 or lower(coalesce(tipo, '')) like '%transportes%'
            then 'TRANSPORTE_CICLOVIAS'
            
            when lower(coalesce(tipo, '')) like '%trabalho e emprego%'
                 or lower(coalesce(tipo, '')) like '%pessoas com deficiência%'
            then 'TRABALHO_INCLUSAO'
            
            when lower(coalesce(tipo, '')) like '%aplicativos%'
                 or lower(coalesce(tipo, '')) like '%nota carioca%'
                 or lower(coalesce(tipo, '')) like '%lei de proteção de dados%'
                 or lower(coalesce(tipo, '')) like '%compras pela internet%'
            then 'SERVICOS_DIGITAIS'
            
            else 'OUTROS_SERVICOS_URBANOS'
        end as subcategoria_interacao,
        
        -- DESCRIÇÃO utilizando o subtipo quando disponível
        coalesce(subtipo, tipo) as descricao_interacao,
        
        -- CANAL
        'CENTRAL_TELEFONICA' as canal_interacao,
        'DIGITAL' as modalidade_interacao,
        
        -- TEMPORAL
        safe_cast(data_inicio as date) as data_interacao,
        safe_cast(data_inicio as datetime) as datahora_inicio,
        safe_cast(data_fim as datetime) as datahora_fim,
        safe_cast(data_particao as date) as data_particao,
        
        -- LOCALIZAÇÃO
        safe_cast(id_bairro as string) as bairro_interacao,
        STRUCT(
            safe_cast(id_logradouro as string) as logradouro,
            safe_cast(numero_logradouro as string) as numero,
            safe_cast(null as string) as complemento,
            safe_cast(id_bairro as string) as bairro,
            safe_cast(null as string) as cep
        ) as endereco_interacao,
        st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64)) as coordenadas,
        
        -- RESULTADO
        safe_cast(status as string) as desfecho_interacao,
        
        -- DADOS FLEXÍVEIS (preservar todos os campos originais disponíveis)
        to_json_string(struct(
            -- Campos adicionais não mapeados
            origem_ocorrencia,
            id_origem_ocorrencia,
            id_territorialidade,
            id_unidade_organizacional,
            nome_unidade_organizacional,
            id_unidade_organizacional_mae,
            unidade_organizacional_ouvidoria,
            categoria,
            id_tipo,
            id_subtipo,
            data_alvo_finalizacao,
            data_alvo_diagnostico,
            data_real_diagnostico,
            tempo_prazo,
            prazo_unidade,
            prazo_tipo,
            dentro_prazo,
            situacao,
            tipo_situacao,
            descricao
        )) as dados_origem
        
    from source_1746
)

select * from interacoes_1746