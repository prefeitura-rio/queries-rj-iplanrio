{{
    config(
        alias='chamado_sla',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(dt_alvo_bonus AS DATETIME) AS dt_alvo_bonus,
    SAFE_CAST(dt_alvo_diagnostico AS DATETIME) AS dt_alvo_diagnostico,
    SAFE_CAST(dt_alvo_finalizacao AS DATETIME) AS dt_alvo_finalizacao,
    SAFE_CAST(dt_atualizacao AS DATETIME) AS dt_atualizacao,
    SAFE_CAST(ic_status_sla_bonus AS STRING) AS ic_status_sla_bonus,
    SAFE_CAST(ic_status_sla_diagnostico AS STRING) AS ic_status_sla_diagnostico,
    SAFE_CAST(ic_status_sla_finalizacao AS STRING) AS ic_status_sla_finalizacao,
    SAFE_CAST(ic_tipo AS STRING) AS ic_tipo,
    SAFE_CAST(ic_tipo_bonus AS STRING) AS ic_tipo_bonus,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_chamado_sla') }}
