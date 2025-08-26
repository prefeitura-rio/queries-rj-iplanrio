{{
    config(
        materialized="incremental",
        schema="intermediario_rmi_conversas",
        tags=["quarter_hourly"],
        unique_key=["id_hsm", "id_disparo", "contato_telefone", "criacao_envio_datahora"],
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}

-- Base consolidada de disparos com metadados de campanha
-- Fonte: telefone_disparado + mensagem_ativa para dados completos

WITH
    source AS (
        SELECT *
        FROM {{ source("brutos_wetalkie_staging", "fluxo_atendimento_*" ) }}
        {% if is_incremental() %}
          WHERE DATETIME(createDate, 'America/Sao_Paulo') >= (
            SELECT MAX(criacao_envio_datahora) 
            FROM {{ this }}
          )
          -- OR createDate >= TIMESTAMP_SUB(
          --   CURRENT_TIMESTAMP(),
          --   INTERVAL 2 DAY
          -- ) -- Safety net para garantir captura
        {% else %}
          -- Carga inicial completa
          WHERE createDate >= '2025-04-18 12:00:00'
        {% endif %}
    ),

    telefone_disparado AS (
        SELECT
            DISTINCT
            CAST(account AS STRING) AS id_conta,
            CAST(templateId AS STRING) AS id_hsm,
            CAST(triggerId AS STRING) AS id_disparo,
            CAST(targetExternalId AS STRING) AS id_externo,
            CAST(replyId AS STRING) AS id_sessao,
            CAST(targetId AS STRING) AS id_contato,
            CAST(flatTarget AS STRING) AS contato_telefone,
            DATETIME(createDate, 'America/Sao_Paulo') AS criacao_envio_datahora,
            DATETIME(sendDate, 'America/Sao_Paulo') AS envio_datahora,
            DATETIME(deliveryDate, 'America/Sao_Paulo') AS entrega_datahora,
            DATETIME(readDate, 'America/Sao_Paulo') AS leitura_datahora,
            DATETIME(failedDate, 'America/Sao_Paulo') AS falha_datahora,
            DATETIME(replyDate, 'America/Sao_Paulo') AS resposta_datahora,
            faultDescription AS descricao_falha,
            LOWER(status) AS status_disparo,
            CASE
                WHEN status = "PROCESSING" THEN 1
                WHEN status = "SENT" THEN 2
                WHEN status = "DELIVERED" THEN 3
                WHEN status = "READ" THEN 4
                WHEN status = "FAILED" THEN 5
            END AS id_status_disparo,
            if(failedDate is not null, true, false) as indicador_falha,
            DATETIME(datarelay_timestamp, 'America/Sao_Paulo') AS datarelay_datahora,
            CAST(EXTRACT(YEAR FROM DATETIME(sendDate, 'America/Sao_Paulo')) AS STRING) AS ano_particao,
            CAST(EXTRACT(MONTH FROM DATETIME(sendDate, 'America/Sao_Paulo')) AS STRING) AS mes_particao,
            DATE(DATETIME(sendDate, 'America/Sao_Paulo')) AS data_particao,
            ROW_NUMBER() OVER (
                PARTITION BY templateId, flatTarget, triggerId, createDate 
                ORDER BY 
                    CASE
                        WHEN faultDescription IS NOT NULL THEN 0
                        WHEN replyId IS NOT NULL THEN 0 ELSE 1 END, -- Prioriza não-nulos
                    datarelay_timestamp DESC
            ) AS rn
        FROM source
    )

select 
    td.id_conta,
    td.id_hsm,
    td.id_disparo,
    td.id_sessao,
    td.id_externo,
    td.id_contato,
    td.contato_telefone,
    ma.nome_hsm,
    ma.nome_campanha as nome_campanha,
    ma.orgao as orgao_responsavel,
    ma.categoria as categoria_hsm,
    ma.ambiente,
    td.criacao_envio_datahora,
    td.envio_datahora,
    td.entrega_datahora,
    td.leitura_datahora,
    td.falha_datahora,
    td.resposta_datahora,
    (
        SELECT MAX(datahora)
        FROM UNNEST([
        td.criacao_envio_datahora,
        td.envio_datahora,
        td.entrega_datahora,
        td.leitura_datahora,
        td.falha_datahora,
        td.resposta_datahora
        ]) AS datahora
    ) AS fim_datahora,
    td.datarelay_datahora,
    td.descricao_falha,
    td.indicador_falha,
    td.id_status_disparo,
    td.status_disparo,
    td.ano_particao,
    td.mes_particao,
    td.data_particao

from telefone_disparado td
left join {{ ref( "raw_iplanrio_wetalkie__mensagem_ativa" ) }} ma
    on td.id_hsm = ma.id_hsm
WHERE rn = 1
