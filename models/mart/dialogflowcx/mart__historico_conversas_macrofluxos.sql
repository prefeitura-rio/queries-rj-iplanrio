{{
    config(
        alias='historico_conversas_macrofluxos'
    )
}}

WITH marked_conversations AS (
  WITH filtro_macrofluxos AS (
    SELECT DISTINCT
        h.conversation_name
    FROM rj-chatbot-dev.dialogflowcx.historico_conversas as h
    LEFT JOIN (
      SELECT
        conversation_name,
        turn_position,
        COUNT(*) AS contagem
      FROM rj-chatbot-dev.dialogflowcx.historico_conversas
      GROUP BY conversation_name, turn_position
      HAVING COUNT(*) > 1
    ) AS bc
      ON h.conversation_name = bc.conversation_name
    WHERE
        h.turn_position = 1
        AND
        (
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) IN (
              "Cuidados com a Cidade",
              "Educação, Saúde e Cidadania",
              "Servidor Público",
              "Suporte Técnico",
              "Trânsito e Transportes",
              "Trânsito",
              "Transportes",
              "Tributos e Licenciamento"
          )
          OR
          (
            JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentPage.displayName')) = 'Menu Principal'
            AND request_time >= '2024-08-02'
          )
          OR
          (
            JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.match.matchType')) = "PLAYBOOK"
          )
        )
        AND bc.conversation_name IS NULL
  ), --travazap filter

  historico AS (
      SELECT
          h.conversation_name,
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.text')) AS mensagem_cidadao,
          JSON_VALUE(JSON_EXTRACT(derived_data, '$.agentUtterances')) AS resposta_bot,
          turn_position,
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentFlow.displayName')) AS fluxo,
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.currentPage.displayName')) AS passo,
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.codigo_servico_1746')) AS codigo_servico_1746,
          JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.macrotema')) AS macrotema,
          `rj-chatbot-dev.dialogflowcx`.get_service_data(JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.codigo_servico_1746'))) AS service_data_array,
          FORMAT_DATETIME("%d/%m/%Y às %H:%M", DATETIME(request_time, "America/Buenos_Aires")) AS horario,
          JSON_EXTRACT(response, '$.queryResult.parameters') AS parametros,
          request_time,
          response,
          ROW_NUMBER() OVER (PARTITION BY h.conversation_name, h.turn_position ORDER BY request_time DESC) AS row_num
      FROM rj-chatbot-dev.dialogflowcx.historico_conversas AS h
      INNER JOIN filtro_macrofluxos AS f
          ON f.conversation_name = h.conversation_name
      -- Remove o ORDER BY aqui e adicione apenas onde for necessário
  ),

  historico_with_lag AS (
    SELECT
      *,
      LAG(codigo_servico_1746) OVER(PARTITION BY conversation_name ORDER BY turn_position) AS lag_codigo_servico_1746_1,
      LAG(codigo_servico_1746, 2) OVER(PARTITION BY conversation_name ORDER BY turn_position) AS lag_codigo_servico_1746_2
    FROM historico
    WHERE row_num = 1 #fitro para garantir apenas 1 linha por par (conversation_name, turn_position)
  ),

  MarkedConversations AS (
      SELECT
          * EXCEPT(service_data_array),
          CASE
            WHEN service_data_array[OFFSET(0)] = "Serviço não mapeado"
            THEN JSON_VALUE(JSON_EXTRACT(response, '$.queryResult.parameters.servico_1746_descricao'))
            ELSE service_data_array[OFFSET(0)] END AS nome_servico_1746,
          service_data_array[OFFSET(1)] AS estimativa_turnos_servico,
          service_data_array[OFFSET(2)] AS estimativa_turnos_menu,
          CASE
              WHEN lag_codigo_servico_1746_2 IS NOT NULL AND codigo_servico_1746 IS NULL THEN 1
              WHEN lag_codigo_servico_1746_1 IS NOT NULL AND lag_codigo_servico_1746_1 != codigo_servico_1746 THEN 1
              WHEN lag_codigo_servico_1746_2 IS NOT NULL AND lag_codigo_servico_1746_1 IS NULL AND codigo_servico_1746 IS NOT NULL THEN 1
              WHEN lag_codigo_servico_1746_1 IS NULL AND codigo_servico_1746 IS NOT NULL THEN 0
              ELSE 0
          END AS isNewPart
      FROM historico_with_lag
  )

  SELECT
    *
  FROM MarkedConversations
),

NumberedConversations AS (
    SELECT
        conversation_name,
        turn_position,
        fluxo,
        codigo_servico_1746,
        nome_servico_1746,
        estimativa_turnos_servico,
        estimativa_turnos_menu,
        SUM(isNewPart) OVER(PARTITION BY conversation_name ORDER BY turn_position) AS part_number
    FROM
        marked_conversations
),
ConversationsWithNewTurn AS (
    SELECT
        conversation_name,
        turn_position,
        part_number,
        fluxo,
        codigo_servico_1746,
        nome_servico_1746,
        estimativa_turnos_servico,
        estimativa_turnos_menu,
        ROW_NUMBER() OVER(PARTITION BY conversation_name, part_number ORDER BY turn_position) AS new_turn
    FROM
        NumberedConversations
),
new_conv_id AS (
    SELECT
        CONCAT(conversation_name, '-', FORMAT('%02d', part_number)) AS new_conversation_id,
        #mensagem_cidadao,
        #resposta_bot,
        turn_position,
        #macrotema,
        fluxo,
        #passo,
        codigo_servico_1746,
        nome_servico_1746,
        estimativa_turnos_servico,
        estimativa_turnos_menu,
        #horario,
        #parametros,
        #request_time,
        #response,
        new_turn,
        conversation_name
    FROM
        ConversationsWithNewTurn
),

service_names AS (
    SELECT
        new_conversation_id,
        CASE
            WHEN COUNT(DISTINCT nome_servico_1746) = 1 THEN MAX(nome_servico_1746)
            WHEN COUNT(DISTINCT nome_servico_1746) = 2 AND MAX(nome_servico_1746) = 'Serviço não mapeado' THEN MIN(nome_servico_1746)
            WHEN COUNT(DISTINCT nome_servico_1746) = 2 AND MIN(nome_servico_1746) = 'Serviço não mapeado' THEN MAX(nome_servico_1746)
            ELSE CONCAT('Conversa com mais de um fluxo: ', STRING_AGG(DISTINCT nome_servico_1746, ', '))
        END AS nome_servico,
        MAX(SAFE_CAST(estimativa_turnos_servico AS INT64)) as estimativa_turnos_servico,
        MAX(SAFE_CAST(estimativa_turnos_menu AS INT64)) as estimativa_turnos_menu
    FROM
        new_conv_id
    GROUP BY
        new_conversation_id
),

first_service_turn AS (
    SELECT
        new_conversation_id,
        MIN(new_turn) AS first_service_turn_position
    FROM
        new_conv_id
    WHERE
        codigo_servico_1746 IS NOT NULL
    GROUP BY
        new_conversation_id
),

compilation_0 AS (
SELECT
    n.new_conversation_id,
    #INITCAP(n.macrotema) as macrotema,
    CASE
      WHEN LOWER(nome_servico) LIKE "%matricula%"
        OR LOWER(nome_servico) LIKE "%matrícula%"
      THEN "Matrícula Municipal"
      WHEN LOWER(nome_servico) LIKE "%cadunico%"
        OR LOWER(nome_servico) LIKE "%cadúnico%"
      THEN "CadÚnico"
    ELSE INITCAP(s.nome_servico) END as nome_servico_1746,
    #n.mensagem_cidadao,
    #n.resposta_bot,
    n.turn_position,
    n.fluxo,
    #n.passo,
    n.codigo_servico_1746,
    #n.horario,
    #n.parametros,
    #n.request_time,
    #n.response,
    n.new_turn,
    n.conversation_name,
    COALESCE(f.first_service_turn_position - 1, 0) AS turns_until_service,
    CASE WHEN n.fluxo = "Coleta Endereço" THEN true ELSE false END AS address_turn,
    CASE WHEN n.fluxo = "Identificação" THEN true ELSE false END AS identification_turn,
    s.estimativa_turnos_servico,
    s.estimativa_turnos_menu
FROM new_conv_id as n
LEFT JOIN service_names as s
    ON n.new_conversation_id = s.new_conversation_id
LEFT JOIN first_service_turn as f
    ON n.new_conversation_id = f.new_conversation_id
),

conversas_completas_metrics AS (
  SELECT
    c0.conversation_name,
    MAX(sq.turns_per_service) AS conversa_completa_turnos_em_menus,
    COUNT(DISTINCT c0.nome_servico_1746) AS conversa_completa_fluxos_interagidos,
    COUNT(CASE WHEN address_turn IS true THEN 1 ELSE NULL END) as conversa_completa_fluxos_endereco,
    COUNT(CASE WHEN identification_turn IS true THEN 1 ELSE NULL END) as conversa_completa_fluxos_identificacao
  FROM compilation_0 as c0
  INNER JOIN
  (SELECT conversation_name, SUM(turns_per_service) as turns_per_service FROM
    (
      SELECT
        conversation_name,
        new_conversation_id,
        MAX(turns_until_service) AS turns_per_service,
      FROM
        compilation_0
      GROUP BY 1,2
    ) GROUP BY 1
  ) AS sq
  ON c0.conversation_name = sq.conversation_name
  GROUP BY
    conversation_name
),

conversas_completas_last_service AS (
  SELECT
    c.conversation_name,
    c.nome_servico_1746 AS ultimo_fluxo_servico,
    ltp.last_turn_position
  FROM
    compilation_0 as c
  INNER JOIN
    (
      SELECT
        conversation_name,
        MAX(turn_position) AS last_turn_position
      FROM
        compilation_0
      GROUP BY
        conversation_name
    ) as ltp
    ON c.conversation_name = ltp.conversation_name AND c.turn_position = ltp.last_turn_position
),

conversas_completas AS (
  SELECT
    m.conversation_name,
    m.conversa_completa_turnos_em_menus,
    m.conversa_completa_fluxos_interagidos,
    m.conversa_completa_fluxos_endereco,
    m.conversa_completa_fluxos_identificacao,
    l.last_turn_position,
    l.ultimo_fluxo_servico
  FROM
    conversas_completas_metrics m
  LEFT JOIN
    conversas_completas_last_service l
  ON
    m.conversation_name = l.conversation_name
),

turnos_em_identificacao_e_endereco AS (
  SELECT
      new_conversation_id,
      COUNT(CASE WHEN address_turn IS true THEN 1 ELSE NULL END) as turnos_em_endereco,
      COUNT(CASE WHEN identification_turn IS true THEN 1 ELSE NULL END) as turnos_em_identificacao
  FROM compilation_0
  GROUP BY 1
),

compilation AS (
  SELECT
    c.*,
    INITCAP(n.macrotema) as macrotema,
    n.mensagem_cidadao,
    n.resposta_bot,
    n.passo,
    n.horario,
    n.parametros,
    n.request_time,
    n.response,
    tie.turnos_em_endereco,
    tie.turnos_em_identificacao,
    cc.conversa_completa_turnos_em_menus,
    cc.conversa_completa_fluxos_interagidos,
    cc.last_turn_position as conversa_completa_duracao,
    cc.ultimo_fluxo_servico as conversa_completa_ultimo_fluxo_servico
  FROM compilation_0 as c
  LEFT JOIN conversas_completas as cc
    ON c.conversation_name = cc.conversation_name
  LEFT JOIN turnos_em_identificacao_e_endereco as tie
    ON c.new_conversation_id = tie.new_conversation_id
  INNER JOIN
    (SELECT * FROM marked_conversations) as n
    ON c.conversation_name = n.conversation_name AND c.turn_position = n.turn_position
)

SELECT * from compilation