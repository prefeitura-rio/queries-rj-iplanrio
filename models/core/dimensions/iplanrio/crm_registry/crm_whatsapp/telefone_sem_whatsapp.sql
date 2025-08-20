{{ config(alias="telefone_sem_whatsapp", schema="crm_whatsapp", materialized="table", tags=["hourly"]) }}

WITH 
  celulares_sem_whatsapp AS (
    SELECT
        CAST(flatTarget AS STRING) as contato_telefone,
        MAX(DATE(DATE_TRUNC(sendDate, DAy))) as data_atualizacao
    FROM {{ source("brutos_wetalkie_staging", "fluxo_atendimento_*") }}
    WHERE failedDate IS NOT NULL AND faultDescription LIKE "%131026%"
    GROUP BY contato_telefone
  )

SELECT * FROM celulares_sem_whatsapp
