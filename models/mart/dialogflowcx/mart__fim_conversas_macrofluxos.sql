{{
    config(
        alias='fim_conversas_macrofluxos'
    )
}}

WITH compilation AS (
  SELECT
    *
  FROM {{ ref('mart__historico_conversas_macrofluxos') }}
),

-- SELECT * FROM compilation
-- where
-- #NOT ENDS_WITH(new_conversation_id, "00")
-- STARTS_WITH(new_conversation_id, "projects/rj-chatbot-dev/locations/global/agents/29358e97-22d5-48e0-b6e0-fe32e70b67cd/environments/f288d64a-52f3-42f7-be7d-cac0b0f4957a/sessions/protocol-09700003664811")
-- order by conversation_name, turn_position ASC

primeira_interacao AS (
  SELECT
    new_conversation_id,
    request_time AS hora_primeira_interacao,
    mensagem_cidadao AS primeira_mensagem,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.intent.displayName')) as primeiro_intent,
    JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.ambiente')) AS ambiente
  FROM compilation
  WHERE new_turn = 1
)

, ultima_interacao as (
SELECT
  new_conversation_id,
  MAX(new_turn) as last_turn
FROM compilation
GROUP BY new_conversation_id
)

, penultimo_turno_0 as (
SELECT
  new_conversation_id,
  MAX(new_turn) as penultimate_turn,
FROM compilation
WHERE new_turn < (
    SELECT MAX(new_turn)
    FROM compilation AS c2
    WHERE c2.new_conversation_id = compilation.new_conversation_id
  )
GROUP BY new_conversation_id
)

, penultimo_passo as (
SELECT
  p0.new_conversation_id,
  penultimate_turn,
  c.passo
FROM penultimo_turno_0 as p0
INNER JOIN compilation as c
  ON p0.penultimate_turn = c.new_turn AND p0.new_conversation_id = c.new_conversation_id
),

fim_conversas_macrofluxo AS (
SELECT
  hist.new_conversation_id as conversation_name,
  JSON_VALUE(parametros, '$.usuario_cpf') AS cpf,
  JSON_VALUE(parametros, '$.phone') as telefone,
  CASE
    WHEN pi.ambiente = "production" THEN "Produção"
    ELSE "Homologação"
  END ambiente,
  CASE
    WHEN hist.nome_servico_1746 = "Serviço Não Mapeado"
      THEN CONCAT("Menu ", JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.macrotema')))
    ELSE hist.nome_servico_1746 END as fluxo_nome,
  horario,
  DATETIME(request_time, "America/Buenos_Aires") as request_time,
  ROUND(DATE_DIFF(request_time, hora_primeira_interacao, SECOND)/60,1) as duracao_minutos,
  hist.new_turn as ultimo_turno,
  pi.primeira_mensagem,
  response,
  CASE
    WHEN hist.passo = "End Session" OR pi.primeiro_intent = "voltar_inicio_padrao" THEN true
    ELSE false
    END conversa_finalizada,
#####
  CASE
    WHEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_protocolo')) IS NOT NULL THEN "chamado_aberto"
    WHEN hist.turn_position = 1 THEN "hard_bounce"
    WHEN
        -- hist.turn_position > 2
        (hist.nome_servico_1746 IN ("Serviço Não Mapeado")
          AND hist.fluxo = "CadÚnico")
        OR
        (hist.nome_servico_1746 IN ("CadÚnico", "Matrícula Municipal"))
        THEN "engajado"
    WHEN
      JSON_VALUE(JSON_EXTRACT(hist.response, '$.queryResult.intent.displayName')) = "encerra_sessao_padrao"
      AND hist.codigo_servico_1746 IS NULL
      THEN "soft_bounce"
    WHEN
      (JSON_VALUE(JSON_EXTRACT(hist.response, '$.queryResult.intent.displayName')) = "voltar_inicio_padrao"
      OR JSON_VALUE(JSON_EXTRACT(hist.response, '$.queryResult.intent.displayName')) = "encerra_sessao_padrao")
      AND hist.codigo_servico_1746 IS NOT NULL
      THEN "desistência"
    WHEN
      JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) = "erro_interno_timeout"
      THEN "timeout SGRC"
    WHEN ENDS_WITH(resposta_bot,'TRANSBORDO') THEN "transbordo"
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
      resposta_bot IS NULL
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) = "erro_interno"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_endereco_abertura_chamado_justificativa')) = "erro_desconhecido"
      OR JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.rebi_elegibilidade_abertura_chamado_justificativa')) = "erro_desconhecido"
      THEN "timeout interno"
    WHEN
      ENDS_WITH(resposta_bot, 'SAIR')
      OR hist.passo = "Finalizar Atendimento"
      OR pen.passo = "Finalizar Atendimento"  -- Verificação do penúltimo turno
      THEN "engajado"
    WHEN
      hist.codigo_servico_1746 IS NOT NULL
      OR hist.nome_servico_1746 != "Serviço Não Mapeado"
      THEN "timeout_usuario_pos_transacao"
    WHEN
      hist.codigo_servico_1746 IS NULL
      THEN "timeout_usuario_pre_transacao" -- "soft_bounce"
    ELSE  "nao_classificado"
    END classificacao_conversa,
#####
  "sucesso" as status_final_conversa,
  hist.passo as passo_falha,
  hist.fluxo as fluxo_falha,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_retorno')) erro_abertura_ticket,
  JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.solicitacao_protocolo')) protocolo,
  CASE
    WHEN hist.turns_until_service = 0
      THEN hist.new_turn
    ELSE hist.turns_until_service END as turnos_em_menus,
  hist.estimativa_turnos_menu,
  hist.new_turn - hist.turnos_em_endereco - hist.turnos_em_identificacao -
    (CASE
      WHEN hist.turns_until_service = 0
        THEN hist.new_turn
      ELSE hist.turns_until_service END) as turnos_em_servico,
  hist.estimativa_turnos_servico,
  hist.turnos_em_endereco,
  hist.turnos_em_identificacao,
  hist.conversation_name as conversa_completa_id,
  CASE
    WHEN hist.conversa_completa_turnos_em_menus = 0
      THEN hist.conversa_completa_duracao
    ELSE hist.conversa_completa_turnos_em_menus END as conversa_completa_turnos_em_menus,
  hist.conversa_completa_fluxos_interagidos,
  hist.conversa_completa_duracao,
  hist.conversa_completa_ultimo_fluxo_servico
FROM compilation as hist
INNER JOIN ultima_interacao as ui
  ON hist.new_conversation_id = ui.new_conversation_id AND hist.new_turn = ui.last_turn
INNER JOIN primeira_interacao as pi
  ON hist.new_conversation_id = pi.new_conversation_id
LEFT JOIN penultimo_passo AS pen
  ON hist.new_conversation_id = pen.new_conversation_id -- Junta com o penúltimo turno
)

SELECT * FROM fim_conversas_macrofluxo
