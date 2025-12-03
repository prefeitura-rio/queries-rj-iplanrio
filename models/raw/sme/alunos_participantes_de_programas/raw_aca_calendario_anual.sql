{{ config(
    materialized='table'
) }}

SELECT
    safe_cast(cal_id AS INT64) AS identificador_calendario,
    safe_cast(ent_id AS INT64) AS identificador_entidade,
    safe_cast(cal_padrao AS BOOL) AS calendario_padrao,
    safe_cast(cal_ano AS INT64) AS calendario_ano,
    safe_cast(cal_descricao AS STRING) AS calendario_descricao,
    safe_cast(cal_dataInicio AS DATE) AS calendario_data_inicio,
    safe_cast(cal_dataFim AS DATE) AS calendario_data_fim,
    safe_cast(cal_situacao AS INT64) AS calendario_situacao,
    safe_cast(cal_dataCriacao AS DATETIME) AS calendario_data_criacao,
    safe_cast(cal_dataAlteracao AS DATETIME) AS calendario_data_alteracao,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'ACA_CalendarioAnual') }}
