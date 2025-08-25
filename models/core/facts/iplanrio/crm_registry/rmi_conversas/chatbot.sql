{{
    config(
        alias="chatbot",
        schema="rmi_conversas",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_interacao",
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        },
        on_schema_change="sync_all_columns",
        tags=["quarter_hourly"]
    )
}}

-- MODELO FINAL: Todas as interações WhatsApp com dados completos
-- Usa intermediate tables para performance e manutenibilidade
-- Estrutura baseada em fct_whatsapp_sessao.sql para boas práticas

with
    disparo as (
        select *
        from {{ ref('int_chatbot_base_disparo') }}
    ),

    receptivo as (
        select *
        from {{ ref('int_chatbot_base_receptivo') }}
    ),

    -- Junção full outer para capturar todas as interações, ativas e receptivas
    interacoes_join as (
        select
            coalesce(d.id_sessao, r.id_sessao) as id_sessao,
            to_hex(md5(coalesce(d.id_sessao, r.id_sessao, CONCAT(d.id_disparo, d.id_contato, CAST(d.criacao_envio_datahora AS STRING))))) as id_interacao,
            d.* except(id_sessao, data_particao, contato_telefone, fim_datahora),
            r.* except(id_sessao, data_particao, inicio_datahora, fim_datahora),
            coalesce(d.contato_telefone, r.contato.telefone) as contato_telefone,
            d.criacao_envio_datahora AS inicio_datahora_ativo,
            d.fim_datahora AS fim_datahora_ativo,
            r.inicio_datahora AS inicio_datahora_receptivo,
            r.fim_datahora AS fim_datahora_receptivo,
            (select min(inicio_interacao_datahora) from unnest([d.criacao_envio_datahora, r.inicio_datahora]) as inicio_interacao_datahora) as inicio_datahora,
            (select max(fim_interacao_datahora) from unnest([d.criacao_envio_datahora, r.fim_datahora]) as fim_interacao_datahora) as fim_datahora,
            coalesce(d.data_particao, r.data_particao) as data_particao
        from disparo as d
        full outer join receptivo as r on d.id_sessao = r.id_sessao
    ),

    -- Unnest e tratamento das mensagens para análise
    mensagens as (
        select
            id_sessao,
            protocolo,
            inicio_datahora as sessao_inicio_datahora,
            mensagem.data,
            mensagem.id as id_mensagem,
            mensagem.fonte,
            mensagem.passo_ura.nome as passo_ura_nome,
            mensagem.texto,
            lower({{ clean_name_string("texto") }}) as texto_limpo
        from interacoes_join, unnest(mensagens) as mensagem
        where array_length(mensagens) > 0
    ),

    -- Adiciona sequencia para análise de fluxo
    sequencia_mensagens as (
        select *,
            row_number() over (partition by id_sessao order by data) as sequencia
        from mensagens
    ),

    ultima_mensagem as (
        select *
        from sequencia_mensagens
        qualify row_number() over (partition by id_sessao order by data desc) = 1
    ),

    -- TIPOS DE ERRO:
    erro_fluxo_travado__usuario_encerra as (
        select distinct id_sessao, 'usuario_encerrou' as tipo_erro
        from ultima_mensagem
        where fonte = 'CUSTOMER' and texto_limpo in ('sair', 'encerrar')
    ),

    erro_fluxo_travado__ultima_mensagem_usuario as (
        select distinct id_sessao, 'fluxo_travado' as tipo_erro
        from ultima_mensagem
        where fonte = 'CUSTOMER' and texto_limpo not in ('sair', 'encerrar')
    ),

    erro_fluxo_travado__usuario_mandou_mensagens_consecutivas as (
        select distinct m1.id_sessao, 'mensagens_consecutivas' as tipo_erro
        from sequencia_mensagens m1
        join sequencia_mensagens m2 on m1.id_sessao = m2.id_sessao
            and m1.sequencia = m2.sequencia - 1
            and m1.fonte = 'CUSTOMER'
            and m2.fonte = 'CUSTOMER'
    ),

    erro_fluxo_travado__loop_ura as (
        select distinct m1.id_sessao, 'loop_ura' as tipo_erro
        from sequencia_mensagens m1
        join sequencia_mensagens m2 on m1.id_sessao = m2.id_sessao
            and m1.sequencia = m2.sequencia - 1
            and m1.fonte = 'URA'
            and m2.fonte = 'URA'
            and m1.texto = m2.texto
    ),

    erro_opcao_invalida as (
        select distinct id_sessao, 'opcao_invalida' as tipo_erro
        from sequencia_mensagens
        where fonte = 'URA' and lower(texto) like '%opcao invalida%'
    ),

    erros_consolidados as (
        select id_sessao, array_agg(distinct tipo_erro order by tipo_erro) as tipo_erro
        from (
            select * from erro_fluxo_travado__usuario_encerra union all
            select * from erro_fluxo_travado__ultima_mensagem_usuario union all
            select * from erro_fluxo_travado__usuario_mandou_mensagens_consecutivas union all
            select * from erro_fluxo_travado__loop_ura union all
            select * from erro_opcao_invalida
        )
        group by 1
    ),

    -- FEEDBACK DAS BUSCAS:
    sessoes_com_busca as (
        select distinct id_sessao
        from sequencia_mensagens
        where passo_ura_nome = '@VLR_RESPOSTA_BUSCA' and fonte = 'URA'
    ),

    feedback_busca as (
        select
            m1.id_sessao,
            struct(
                m1.texto_limpo as pergunta,
                m2.texto_limpo as resposta,
                if(m2.texto_limpo = "nao", m3.texto_limpo, null) as resposta_negativa_complemento
            ) as feedback
        from sequencia_mensagens as m1
        left join sequencia_mensagens as m2 on m1.id_sessao = m2.id_sessao and m1.sequencia + 1 = m2.sequencia
        left join sequencia_mensagens as m3 on m1.id_sessao = m3.id_sessao and m1.sequencia + 3 = m3.sequencia
        where m1.texto_limpo like 'o whatsapp funcionou como esperado' and m2.texto_limpo in ('sim', 'nao')
    ),

    sessoes_com_busca_e_feedback as (
        select s.id_sessao, f.feedback
        from sessoes_com_busca as s
        left join feedback_busca as f using (id_sessao)
    ),

    -- CALCULOS DE TEMPO E ESTATISTICAS
    session_timestamps as (
        select
            id_sessao,
            min(data) as primeira_mensagem_datahora,
            max(case when fonte = 'CUSTOMER' then data else null end) as ultima_mensagem_cliente_datahora
        from sequencia_mensagens
        group by id_sessao
    ),

    message_response_times as (
        select
            id_sessao,
            id_mensagem,
            case
                when fonte = 'CUSTOMER' and lag(fonte) over (partition by id_sessao order by data) = 'URA'
                then timestamp_diff(data, lag(data) over (partition by id_sessao order by data), second)
            end as tempo_resposta_cliente_seg
        from sequencia_mensagens
    ),

    estatisticas as (
        select
            sm.id_sessao,
            struct(
                count(distinct sm.id_mensagem) as total_mensagens,
                count(distinct case when sm.fonte = 'CUSTOMER' then sm.id_mensagem end) as total_mensagens_contato,
                count(distinct case when sm.fonte = 'URA' and sm.passo_ura_nome = '@VLR_RESPOSTA_BUSCA' then sm.id_mensagem end) as total_mensagens_busca,
                -- Tempo efetivo de sessão (do início/hsm até a última mensagem do cliente) 
                max(timestamp_diff(timestamp(st.ultima_mensagem_cliente_datahora), coalesce(timestamp(ij.leitura_datahora), timestamp(sm.sessao_inicio_datahora)), second)) as duracao_sessao_seg,
                -- Tempo efetivo de interação do cliente sessão (do início da interação até a última mensagem do cliente)
                max(timestamp_diff(timestamp(st.ultima_mensagem_cliente_datahora), timestamp(st.primeira_mensagem_datahora), second)) as duracao_interacao_seg,
                -- Tempo efetivo de sessão (do início da URA até a última mensagem do cliente)
                max(timestamp_diff(timestamp(st.ultima_mensagem_cliente_datahora), timestamp(sm.sessao_inicio_datahora), second)) as duracao_ura_seg,
                avg(mrt.tempo_resposta_cliente_seg) as tempo_medio_resposta_cliente_seg
            ) as estatisticas
        from sequencia_mensagens sm
        left join session_timestamps st on sm.id_sessao = st.id_sessao
        left join message_response_times mrt on sm.id_sessao = mrt.id_sessao and sm.id_mensagem = mrt.id_mensagem
        left join interacoes_join ij on sm.id_sessao = ij.id_sessao
        group by 1
    ),

    -- DADOS DE CONTATO
    contatos as (
        select
            contato_telefone,
            safe_cast(cpf as string) as cpf,
            id_contato,
            contato_nome,
            data_optin,
            data_optout
        from (
            select
                *,
                row_number() over (
                    partition by contato_telefone
                    order by data_particao desc
                ) as rn
            from {{ ref('contato') }}
            where contato_telefone is not null
        )
        where rn = 1
    ),

    -- TABELA FINAL
    final as (
        select
            -- CHAVE
            ij.id_interacao,
            ij.id_sessao,
            ij.id_externo,
            ij.protocolo,

            -- TEMPORALIDADE
            ij.inicio_datahora,
            ij.fim_datahora,

            -- CONTATO
            struct(
                coalesce(ij.id_contato, c.id_contato) as id_contato,
                ij.contato_telefone,
                coalesce(ij.contato.nome, c.contato_nome) as contato_nome,
                c.cpf,
                c.data_optin,
                c.data_optout
            ) as contato,

            -- HSM (Disparo Ativo)
            struct(
                if(ij.id_hsm is not null, true, false) as indicador,
                if(ij.falha_datahora is not null, true, false) as indicador_falha,
                ij.id_hsm,
                ij.id_disparo,
                ij.status_disparo,
                ij.nome_campanha,
                ij.categoria_hsm,
                ij.orgao_responsavel,
                ij.ambiente,
                ij.criacao_envio_datahora,
                ij.envio_datahora,
                ij.entrega_datahora,
                ij.leitura_datahora,
                ij.resposta_datahora,
                ij.falha_datahora,
                ij.descricao_falha,
                ij.inicio_datahora_ativo,
                ij.fim_datahora_ativo
            ) as hsm,

            -- ERRO FLUXO
            struct(
                if(err.id_sessao is not null, true, false) as indicador,  -- TODO: errado
                err.tipo_erro
            ) as erro_fluxo,

            -- DADOS DA CONVERSA
            ij.mensagens,
            struct(
                if(b.id_sessao is not null, true, false) as indicador,
                b.feedback
            ) as busca,
            STRUCT(
              if(ij.ura.id is not null, true, false) as indicador,
              ij.ura.id,
              ij.ura.nome,
              ij.inicio_datahora_receptivo,
              ij.fim_datahora_receptivo
            ) AS ura,
            stat.estatisticas,
            ij.operador,
            ij.tabulacao,

            -- INDICADORES BOOLEANOS
            ij.entrega_datahora is not null as foi_entregue,
            ij.leitura_datahora is not null as foi_lida,
            ij.resposta_datahora is not null as foi_respondida,
            ij.id_sessao is not null and ij.inicio_datahora_receptivo is not null as gerou_conversa,

            -- PARTICIONAMENTO E METADADADOS
            ij.data_particao,
            current_datetime() as data_processamento

        from interacoes_join as ij
        left join erros_consolidados as err on ij.id_sessao = err.id_sessao
        left join sessoes_com_busca_e_feedback as b on ij.id_sessao = b.id_sessao
        left join estatisticas as stat on ij.id_sessao = stat.id_sessao
        left join contatos as c on ij.contato_telefone = c.contato_telefone

        -- Filtro incremental
        {% if is_incremental() %}
            where ij.data_particao > (select max(data_particao) from {{ this }})
        {% endif %}
    )

select * from final