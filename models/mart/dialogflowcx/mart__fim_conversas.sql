{{
    config(
        alias='fim_conversas'
    )
}}

with ultima_interacao as (
SELECT
  conversation_name,
  MAX(turn_position) as last_turn
FROM `rj-chatbot-dev.dialogflowcx.historico_conversas`
GROUP BY conversation_name
),

# Captura apenas mensagens vindas do ASC através da mensagem que inicia a conversa
primeira_interacao AS (
  SELECT
    conversation_name,
    `rj-chatbot-dev.dialogflowcx`.inicial_sentence_to_flow_name(JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text'))) as fluxo_primeira_interacao,
    request_time AS hora_primeira_interacao,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')) AS primeira_mensagem,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.ambiente')) AS ambiente
  FROM `rj-chatbot-dev.dialogflowcx.historico_conversas`
  WHERE
    turn_position = 1
    -- AND `rj-chatbot-dev.dialogflowcx.inicial_sentences`(JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')))
    AND JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.match.matchType')) != "PLAYBOOK"
),

duplicate_conversations AS (
  SELECT
    conversation_name,
    turn_position,
    COUNT(*) AS contagem
  FROM `rj-chatbot-dev.dialogflowcx.historico_conversas`
  GROUP BY conversation_name, turn_position
  HAVING COUNT(*) > 1
),
hist AS (
  SELECT
    h.*
  FROM `rj-chatbot-dev.dialogflowcx.historico_conversas` AS h
  LEFT JOIN `rj-chatbot-dev.dialogflowcx.fim_conversas_da` AS da
    ON da.conversa_completa_id = h.conversation_name
  LEFT JOIN `rj-chatbot-dev.dialogflowcx.fim_conversas_macrofluxos` AS mf
    ON mf.conversa_completa_id = h.conversation_name
  LEFT JOIN duplicate_conversations AS bc
    ON h.conversation_name = bc.conversation_name
       AND h.turn_position = bc.turn_position
  WHERE da.conversation_name IS NULL
    AND mf.conversation_name IS NULL
    AND bc.conversation_name IS NULL
),

fim_conversas_1746 AS (
SELECT
  hist.conversation_name,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.usuario_cpf')) AS cpf,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.phone')) as telefone,
  CASE
    WHEN pi.ambiente = "production" THEN "Produção"
    ELSE "Homologação"
  END ambiente,
  INITCAP(pi.fluxo_primeira_interacao) as fluxo_nome,
  FORMAT_DATETIME("%d/%m/%Y às %H:%M", DATETIME(request_time, "America/Buenos_Aires")) as horario,
  DATETIME(request_time, "America/Buenos_Aires") as request_time,
  ROUND(DATE_DIFF(request_time, hora_primeira_interacao, SECOND)/60,1) as duracao_minutos,
  hist.turn_position as ultimo_turno,
  pi.primeira_mensagem,
  response,
  CASE
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentPage.displayName')) = "End Session" THEN true
    ELSE false
    END conversa_finalizada,
  CASE
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_protocolo')) IS NOT NULL THEN "chamado_aberto"
    WHEN hist.turn_position = 1 THEN "hard_bounce"
    WHEN
      hist.turn_position = 2
      AND (
        ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'VOLTAR')
        OR ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'SAIR')
      )
      THEN  "timeout_usuario_pre_transacao" -- "soft_bounce"
    WHEN
      hist.turn_position = 2
      THEN "timeout_usuario_pos_transacao"
    WHEN
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) = "erro_interno_timeout"
      THEN "timeout SGRC"
    WHEN ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')),'TRANSBORDO') THEN "transbordo"
    WHEN
      # Casos em que o Chatbot identificou a inelegibilidade
      (JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado_justificativa')) != "erro_desconhecido"
      AND
      (JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado')) = "false"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado')) = "false")
      )
      # Casos em que o SGRC identificou a inelegibilidade
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado_justificativa')) = "chamado_aberto"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado_justificativa')) = "chamado_fechado_12_dias"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado_justificativa')) = "chamado_aberto"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado_justificativa')) = "chamado_fechado_12_dias"
      THEN "impedimento_regra_negocio"
    WHEN
      JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')) IS NULL
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) = "erro_interno"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado_justificativa')) = "erro_desconhecido"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado_justificativa')) = "erro_desconhecido"
      THEN "timeout interno"
    WHEN
        ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'VOLTAR')
     OR ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'SAIR')
     THEN "desistência"
    ELSE  "timeout_usuario_pos_transacao"
    END classificacao_conversa,
  CASE
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_protocolo')) IS NOT NULL
      OR ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'VOLTAR')
      OR ENDS_WITH(JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')), 'SAIR')
      THEN "sucesso"
    ELSE "falha"
    END status_final_conversa,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentPage.displayName')) as passo_falha,
  CASE
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '1647 (RRL)' THEN 'Remoção de Resíduo'
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '1614 (PAL)' THEN 'Poda de Árvore'
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '1464 (VACIO)' THEN 'Verificação de Ar Condicionado Inoperante em Ônibus'
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '152 (RLU)' THEN 'Reparo de Luminária'
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '182 (RBDAP)' THEN 'Reparo de Buraco, Deformação ou Afundamento na pista'
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '3581 (FEIV)' THEN "Fiscalização de Estacionamento Irregular de Veículo"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '3802 (RSTA)' THEN "Reparo de Sinal de Trânsito Apagado"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '1607 (REBI)' THEN "Remoção de Entulho e Bens Inservíveis"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '192 (DBGRR)' THEN "Desobstrução de bueiros, galerias, ramais de águas pluviais e ralos"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '2569 (RTG)' THEN "Reposição de Tampão ou Grelha"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '223 (RSTAP)' THEN "Reparo de Sinal de Trânsito em Amarelo Piscante"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '1618 (CRCA)' THEN "Controle de Roedores e Caramujos Africanos"
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) = '3803 (RSTAAV)' THEN "Reparo de Sinal de Trânsito Abalroado ou Ausente ou Virado"
    ELSE JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName'))
    END as fluxo_falha,
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
FROM hist
INNER JOIN ultima_interacao as ui
  ON hist.conversation_name = ui.conversation_name AND hist.turn_position = ui.last_turn
INNER JOIN primeira_interacao as pi
  ON hist.conversation_name = pi.conversation_name)

SELECT
  *,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.codigo_servico_1746')) as codigo_servico,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.teste_ab_versao')) as teste_ab_versao,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.nota_pesquisa')) as nota_pesquisa_satisfacao
FROM fim_conversas_1746

UNION ALL

SELECT
  *,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.codigo_servico_1746')) as codigo_servico,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.teste_ab_versao')) as teste_ab_versao,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.nota_pesquisa')) as nota_pesquisa_satisfacao
FROM {{ ref('mart__fim_conversas_da') }}

UNION ALL

SELECT
  *,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.codigo_servico_1746')) as codigo_servico,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.teste_ab_versao')) as teste_ab_versao,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.nota_pesquisa')) as nota_pesquisa_satisfacao
FROM {{ ref('mart__fim_conversas_macrofluxos') }}
