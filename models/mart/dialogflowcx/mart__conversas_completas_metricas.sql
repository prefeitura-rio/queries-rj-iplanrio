{{
    config(
        alias='conversas_completas_metricas'
    )
}}

SELECT
  MIN(request_time) as request_time,
  conversa_completa_id,
  MAX(ambiente) AS ambiente,
  ARRAY_AGG(DISTINCT conversation_name) AS conversation_name_list,
  AVG(conversa_completa_turnos_em_menus) AS conversa_completa_turnos_em_menus,
  MAX(conversa_completa_fluxos_interagidos) AS conversa_completa_fluxos_interagidos,
  MAX(conversa_completa_duracao) AS conversa_completa_duracao,
  MAX(conversa_completa_ultimo_fluxo_servico) AS conversa_completa_ultimo_fluxo_servico
FROM {{ ref('mart__fim_conversas') }}
GROUP BY conversa_completa_id