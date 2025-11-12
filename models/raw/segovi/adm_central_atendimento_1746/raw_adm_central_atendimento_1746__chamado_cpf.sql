{{
    config(
        alias='chamado_cpf',
        schema='adm_central_atendimento_1746',
        materialized='table',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

WITH tb_pessoa AS (
    SELECT
        CAST(id_pessoa AS STRING) AS id_pessoa,
        CAST(ds_cpf AS STRING) AS ds_cpf
    FROM {{ source('brutos_1746_staging_airbyte', 'tb_pessoa') }} AS t
),

tb_protocolo_chamado AS (
    SELECT
        _airbyte_extracted_at,
        CAST(id_chamado_fk AS STRING) AS id_chamado,
        CAST(id_protocolo_fk AS STRING) AS id_protocolo,
        CAST(dt_insercao AS STRING) AS dt_insercao
    FROM {{ source('brutos_1746_staging_airbyte', 'tb_protocolo_chamado') }} AS t
),

tb_protocolo AS (
    SELECT
        CAST(id_protocolo AS STRING) AS id_protocolo,
        CAST(id_pessoa_fk AS STRING) AS id_pessoa,
        CAST(dt_inicio AS STRING) as dt_inicio
    FROM {{ source('brutos_1746_staging_airbyte', 'tb_protocolo') }} AS t
),


source_data AS ( 
    SELECT
        pc.id_chamado,
        CASE WHEN p.ds_cpf IS NOT NULL 
            THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p.ds_cpf,
                '-', ''), '.', ''), '/', ''), '(', ''), ')', ''), ' ', ''), ',', ''), '+', ''), ';', '') 
        ELSE NULL 
        END AS cpf,
        pc._airbyte_extracted_at AS extracted_at,
        SAFE_CAST(DATE_TRUNC(DATE(pr.dt_inicio), month) AS DATE) data_particao
    FROM tb_protocolo_chamado pc
    LEFT JOIN tb_protocolo pr ON pc.id_protocolo = pr.id_protocolo
    LEFT JOIN tb_pessoa p ON pr.id_pessoa = p.id_pessoa
   )

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    cpf, 
    extracted_at,
    data_particao
FROM source_data AS t