{{
  config(
    schema="intermediario_rmi_conversas", 
    materialized='ephemeral',
    tags=["hourly"],
    unique_key='id_contato',
    incremental_strategy='merge',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH
    source as (
        select * from {{ source("brutos_wetalkie_staging", "fluxos_ura") }}),

    fix_json as (
        select
            * except (json_data),
            replace(
                replace(replace(json_data, 'None', 'null'), 'True', 'true'),
                'False',
                'false'
            ) as json_data
        from source
    ),

    ura_contacts_ AS (
        SELECT 
        DISTINCT
            json_extract_scalar(json_data, '$.contact.name') AS contato_nome,
            CAST(json_extract_scalar(json_data, '$.contact.id') AS STRING) AS id_contato,
            MAX(DATE(parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', json_extract_scalar(json_data, '$.beginDate')), 'America/Sao_Paulo')) AS data_update,
            MIN(DATE(parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', json_extract_scalar(json_data, '$.beginDate')), 'America/Sao_Paulo')) AS data_optin,
            MAX(DATE(parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', json_extract_scalar(json_data, '$.beginDate')), 'America/Sao_Paulo')) as data_particao
        FROM fix_json
        WHERE
            DATE(parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', json_extract_scalar(json_data, '$.beginDate')), 'America/Sao_Paulo') >= "2025-05-24"
        GROUP BY 1, 2
    )

SELECT * FROM ura_contacts_
