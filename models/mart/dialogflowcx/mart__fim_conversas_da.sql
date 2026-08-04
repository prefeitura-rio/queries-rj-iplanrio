{{
    config(
        alias='fim_conversas_da'
    )
}}

WITH filtro_divida_ativa AS (
    SELECT
      conversation_name
    FROM `rj-chatbot-dev.dialogflowcx.historico_conversas`
    WHERE
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.ambiente')) = "production"
      AND turn_position = 1
      AND JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')) = "dívida ativa"
),
historico AS (
    SELECT
      h.conversation_name,
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')) as mensagem_cidadao,
      JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')) as resposta_bot,
      turn_position,
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) as fluxo,
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentPage.displayName')) as passo,
      FORMAT_DATETIME("%d/%m/%Y às %H:%M", DATETIME(request_time, "America/Buenos_Aires")) as horario,
      JSON_EXTRACT(response, '$.queryResult.parameters') as parametros,
      request_time,
      response
    FROM `rj-chatbot-dev.dialogflowcx.historico_conversas` as h
    INNER JOIN filtro_divida_ativa as f ON f.conversation_name = h.conversation_name
    ORDER BY conversation_name, request_time ASC
),
MarkedConversations AS (
    SELECT
        *,
        CASE
            WHEN LAG(fluxo, 2) OVER(PARTITION BY conversation_name ORDER BY turn_position) != 'DA 0 - Menu da Dívida Ativa'
                 AND LAG(fluxo) OVER(PARTITION BY conversation_name ORDER BY turn_position) = 'DA 0 - Menu da Dívida Ativa'
            THEN 1
            ELSE 0
        END AS isNewPart
    FROM
        historico
),
NumberedConversations AS (
    SELECT
        *,
        SUM(isNewPart) OVER(PARTITION BY conversation_name ORDER BY turn_position) AS part_number
    FROM
        MarkedConversations
),
ConversationsWithNewTurn AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY conversation_name, part_number ORDER BY turn_position) AS new_turn
    FROM
        NumberedConversations
),
new_conv_id AS (
    SELECT
        CONCAT(conversation_name, '-', FORMAT('%02d', part_number)) AS new_conversation_id,
        mensagem_cidadao,
        resposta_bot,
        turn_position,
        fluxo,
        passo,
        horario,
        parametros,
        request_time,
        response,
        new_turn,
        conversation_name
    FROM
        ConversationsWithNewTurn
),
TypeClassification AS (
    SELECT
        new_conversation_id,
        CASE
            WHEN COUNTIF(fluxo != 'DA 0 - Menu da Dívida Ativa') = 0 THEN 'informational'
            WHEN MAX(CASE WHEN fluxo IN ('DA 1 - Consultar Débitos Dívida Ativa Solicitar Guias Exibir Informações',
                                         'DA 1.7. Emitir Guia de Pagamento à Vista',
                                         'DA 1.9. Emitir Guia de Regularização de Débitos',
                                         'DA 3 - Consulta de protestos',
                                         'DA 5 - Cadastre-se (Serviços úteis)') THEN 1 ELSE 0 END) = 1 THEN 'service'
            WHEN MAX(CASE WHEN fluxo IN ('DA 2 - Como parcelar minha dívida', 'DA 4 - Informações Gerais') THEN 1 ELSE 0 END) = 1 THEN 'informational'
            ELSE 'undefined'
        END AS type_flow
    FROM
        new_conv_id
    GROUP BY
        new_conversation_id
)

, DistinctFluxo AS (
    SELECT DISTINCT
        new_conversation_id,
        fluxo
    FROM
        new_conv_id
    WHERE
        fluxo != 'DA 0 - Menu da Dívida Ativa'
),

FluxoMain AS (
    SELECT
        d.new_conversation_id,
        MIN(d.fluxo) AS main_fluxo,
        STRING_AGG(d.fluxo, ', ') AS distinct_fluxo_list
    FROM
        DistinctFluxo d
    GROUP BY
        d.new_conversation_id
),

ParametrosExtracted AS (
    SELECT
        new_conversation_id,
        CASE
            WHEN JSON_VALUE(parametros, '$.api_resposta_sucesso') IS NULL THEN
                CASE
                    WHEN REGEXP_CONTAINS(TO_JSON_STRING(parametros), r'\"api_resposta_sucesso\"') THEN 'replaced'
                    ELSE NULL
                END
            ELSE JSON_VALUE(parametros, '$.api_resposta_sucesso')
        END AS api_resposta_sucesso,
        CASE
            WHEN JSON_VALUE(parametros, '$.api_descricao_erro') IS NULL THEN
                CASE
                    WHEN REGEXP_CONTAINS(TO_JSON_STRING(parametros), r'\"api_descricao_erro\"') THEN 'replaced'
                    ELSE NULL
                END
            ELSE JSON_VALUE(parametros, '$.api_descricao_erro')
        END AS api_descricao_erro
    FROM
        new_conv_id
    WHERE
        parametros IS NOT NULL
),

DistinctApiValues AS (
    SELECT
        new_conversation_id,
        CASE
            WHEN COUNTIF(api_resposta_sucesso = 'false') > 0 THEN 'false'
            WHEN COUNTIF(api_resposta_sucesso = 'true') > 0 THEN 'true'
            ELSE MAX(api_resposta_sucesso)  -- Handles 'replaced' or NULL
        END AS api_resposta_sucesso,
        MAX(api_descricao_erro) AS api_descricao_erro
    FROM
        ParametrosExtracted
    GROUP BY
        new_conversation_id
),

FinalTableDA AS (
    SELECT
        n.*,
        type_flow,
        IFNULL(f.main_fluxo, 'DA 0 - Menu da Dívida Ativa') AS main_fluxo,
        a.api_resposta_sucesso,
        a.api_descricao_erro
    FROM
        new_conv_id n
    JOIN
        TypeClassification t ON n.new_conversation_id = t.new_conversation_id
    LEFT JOIN
        FluxoMain f ON n.new_conversation_id = f.new_conversation_id
    LEFT JOIN
        DistinctApiValues a ON n.new_conversation_id = a.new_conversation_id
    WHERE t.type_flow != 'undefined'
)

-- SELECT * FROM FinalTableDA #WHERE api_resposta_sucesso = "replaced" AND main_fluxo IN ('DA 3 - Consulta de protestos', 'DA 1 - Consultar Débitos Dívida Ativa Solicitar Guias Exibir Informações')
-- ORDER BY new_conversation_id, new_turn;

, FinalTableOutros AS (
    # Separando fluxos de outros serviços em que o usuário começou na dívida ativa mas foi para outro serviço depois
    SELECT
        n.*,
        t.type_flow,
        f.distinct_fluxo_list
    FROM
        new_conv_id n
    JOIN
        TypeClassification t ON n.new_conversation_id = t.new_conversation_id
    LEFT JOIN
        FluxoMain f ON n.new_conversation_id = f.new_conversation_id
    WHERE t.type_flow = 'undefined'
)

, primeira_interacao AS (
  SELECT
    new_conversation_id,
    main_fluxo as fluxo_primeira_interacao,
    request_time AS hora_primeira_interacao,
    mensagem_cidadao AS primeira_mensagem,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.ambiente')) AS ambiente
  FROM FinalTableDA
  WHERE turn_position = 1
)

, ultima_interacao as (
SELECT
  new_conversation_id,
  MAX(new_turn) as last_turn
FROM FinalTableDA
GROUP BY new_conversation_id
),

fim_conversas_DA AS (
SELECT
  hist.new_conversation_id as conversation_name,
  JSON_VALUE(parametros, '$.usuario_cpf') AS cpf,
  JSON_VALUE(parametros, '$.phone') as telefone,
  CASE
    WHEN pi.ambiente = "production" THEN "Produção"
    ELSE "Homologação"
  END ambiente,
  main_fluxo as fluxo_nome,
  horario,
  DATETIME(request_time, "America/Buenos_Aires") as request_time,
  ROUND(DATE_DIFF(request_time, hora_primeira_interacao, SECOND)/60,1) as duracao_minutos,
  hist.new_turn as ultimo_turno,
  pi.primeira_mensagem,
  response,
  CASE
    WHEN hist.passo = "End Session" OR hist.fluxo = "DA 0 - Menu da Dívida Ativa" THEN true
    ELSE false
    END conversa_finalizada,
#####
  CASE
    WHEN hist.new_turn = 1 THEN "hard_bounce"
    WHEN
      hist.new_turn = 2
      AND (
        ENDS_WITH(hist.resposta_bot, 'VOLTAR')
        OR ENDS_WITH(hist.resposta_bot, 'SAIR')
      )
      THEN "timeout_usuario_pre_transacao" -- "soft_bounce"
    WHEN
        ENDS_WITH(hist.resposta_bot, 'VOLTAR')
     OR ENDS_WITH(hist.resposta_bot, 'SAIR')
     THEN "desistencia"
    WHEN
        (hist.type_flow = "informational" AND hist.fluxo = "DA 0 - Menu da Dívida Ativa")
        THEN "engajado"
    WHEN
      (hist.type_flow = "informational" AND hist.fluxo != "DA 0 - Menu da Dívida Ativa") #timeout informacional
      OR
      (hist.type_flow = "service" AND hist.api_resposta_sucesso IS NULL)
      THEN "timeout_usuario_pre_transacao"
    -- WHEN
    --   JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado')) = "false"
    --   OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado')) = "false"
    --   THEN "impedimento_regra_negocio"
    WHEN
        (hist.type_flow = "service" AND hist.api_resposta_sucesso IN ("replaced", "true"))
        THEN "transacao_realizada"
    WHEN
      hist.resposta_bot IS NULL
      THEN "timeout interno"
    WHEN hist.type_flow = "service" THEN "timeout_usuario_pos_transacao"
    ELSE "investigar"
    END classificacao_conversa,
#####
  "sucesso" as status_final_conversa,
  hist.passo as passo_falha,
  hist.fluxo as fluxo_falha,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) erro_abertura_ticket,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_protocolo')) protocolo,
  SAFE_CAST(NULL AS INT64) AS turnos_em_menus,
  SAFE_CAST(NULL AS INT64) AS estimativa_turnos_menu,
  SAFE_CAST(NULL AS INT64) AS turnos_em_servico,
  SAFE_CAST(NULL AS INT64) AS estimativa_turnos_servico,
  SAFE_CAST(NULL AS INT64) AS turnos_em_endereco,
  SAFE_CAST(NULL AS INT64) AS turnos_em_identificacao,
  hist.conversation_name as conversa_completa_id,
  SAFE_CAST(NULL AS INT64) AS conversa_completa_turnos_em_menus,
  SAFE_CAST(NULL AS INT64) AS conversa_completa_fluxos_interagidos,
  SAFE_CAST(NULL AS INT64) AS conversa_completa_duracao,
  SAFE_CAST(NULL AS STRING) AS conversa_completa_ultimo_fluxo_servico,
FROM FinalTableDA as hist
INNER JOIN ultima_interacao as ui
  ON hist.new_conversation_id = ui.new_conversation_id AND hist.new_turn = ui.last_turn
INNER JOIN primeira_interacao as pi
  ON hist.new_conversation_id = pi.new_conversation_id
ORDER BY request_time DESC)

SELECT * FROM fim_conversas_DA WHERE fluxo_nome != 'DA 0 - Menu da Dívida Ativa'