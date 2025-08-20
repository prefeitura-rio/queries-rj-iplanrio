{{ config(alias="base_mensagem_ativa", schema="crm_whatsapp", materialized="ephemeral") }}

WITH templates AS (
  SELECT templateId
  FROM UNNEST(GENERATE_ARRAY(1, 200)) AS templateId

)
SELECT 
CAST(templateId AS STRING) AS id_hsm,
CASE
  WHEN templateId = 1 THEN "WebSummit"
  WHEN templateId = 2 THEN "Ouvidores PCRJ"
  WHEN templateId = 4 THEN "EAI-GPT - REUNIÃO COM PREFEITO [HOM]"
  WHEN templateId = 6 THEN "EAI-GPT - REUNIÃO COM PREFEITO [HOM - SEM BOTÕES]"
  WHEN templateId = 7 THEN "EAI-GPT - REUNIÃO COM PREFEITO [PROD - SEM BOTÕES]"
  WHEN templateId = 8 THEN "EAI-GPT - REUNIÃO COM PREFEITO [PROD - SEM BOTÕES] v2"
  WHEN templateId = 9 THEN "EAI-GPT - REUNIÃO COM PREFEITO [HOM - SEM BOTÕES] v2"
  WHEN templateId = 10 THEN "EAI-GPT - REUNIÃO COM PREFEITO [HOM] v3"
  WHEN templateId = 11 THEN "EAI-GPT - REUNIÃO COM PREFEITO [HOM] v4"
  WHEN templateId = 12 THEN "EAI-GPT - REUNIÃO COM PREFEITO [PROD - SEM BOTÕES] v3"
  WHEN templateId = 13 THEN "SME - Frequência escolar [HOM]"
  WHEN templateId = 14 THEN "SME - Frequência escolar [HOM] v2"
  WHEN templateId = 15 THEN "SME - Frequência escolar [HOM] v3"
  WHEN templateId = 16 THEN "SME - Frequência escolar [HOM] v4"
  WHEN templateId = 17 THEN "SME - Frequência escolar [PROD] v1"
  WHEN templateId = 18 THEN "SISREG - Lembrete agendamento [HOM] v1"
  WHEN templateId in (19, 20) THEN "Autenticação [HOM]"
  WHEN templateId = 21 THEN "SMS - CSAT Atendimento [HOM] v1"
  WHEN templateId = 22 THEN "SMS Confirmação SISREG [HOM] v1"
  WHEN templateId = 23 THEN "SMS Confirmação SISREG [HOM] v2"
  WHEN templateId = 24 THEN "SMS Confirmação SISREG [PROD] v3"
  WHEN templateId = 31 THEN "Agendamento Cadúnico"
  WHEN templateId = 59 THEN "SMTR - Mudança Sistema Jaé (com nome)"
  WHEN templateId = 100 THEN "smtr-jae-prod-v10"
  WHEN templateId = 101 THEN "smas-agendcadunico-prod-v1"
  WHEN templateId = 113 THEN "sme-abandono-escolar-prod-v3"
  ELSE "Teste"
END AS nome_hsm,
CASE
  WHEN templateId IN (1) THEN "dev"
  WHEN templateId IN (4, 6, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23) THEN "hom"
  WHEN templateId IN (7, 8, 12, 17, 24, 31, 59, 100, 101, 113) THEN "prod"
  ELSE NULL
END AS ambiente,
CASE
  WHEN templateId IN (14, 16, 17, 18, 21, 22, 23, 24, 31, 59, 100, 101, 113) THEN "Utilidade"
  WHEN templateId IN (1, 2, 4, 6, 7, 8, 9, 10, 11, 12, 13, 15) THEN "Marketing"
  WHEN templateId IN (19, 20) THEN "Autenticação"
  ELSE NULL
END AS categoria,
CASE
  WHEN templateId IN (1, 4, 6, 7, 8, 9, 10, 11, 12) THEN "IPLAN"
  WHEN templateId = 2 THEN "Ouvidores PCRJ"
  WHEN templateId IN (13, 14, 15, 16, 17, 113) THEN "SME"
  WHEN templateId IN (18, 21, 22, 23, 24) THEN "SMS"
  WHEN templateId IN (31, 101) THEN "SMAS"
  WHEN templateId IN (59, 100) THEN "SMTR"
  WHEN templateId in (19, 20) THEN "Autenticação"
  ELSE "Teste"
END AS orgao,
CAST(NULL AS STRING) as nome_campanha


FROM templates
ORDER BY CAST(id_hsm AS INT64)