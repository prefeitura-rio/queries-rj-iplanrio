{{
  config(
    materialized='table',
    alias='orgao_tramitacao_processo'
  )
}}

SELECT
codOrgaoSMU AS Id_Orgao_SMU,
codOrgaoSici AS cod_Orgao_SICI,
sigla AS Sigla,
nomOrgao AS Orgao,
CAST(cancelado AS BOOL) AS Cancelado,
COD_Secretaria AS Id_Secretaria,
CAST(permiteAgendamento AS BOOL) AS Permite_Agendamento,

FROM {{ source('adm_licenca_urbanismo_staging', 'orgao_tramitacao_processo') }}
