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
        CAST(id_protocolo_chamado AS STRING) AS id_protocolo_chamado,
        CAST(ic_motivo AS STRING) AS ic_motivo,
        CAST(dt_insercao AS STRING) AS dt_insercao
    FROM {{ source('brutos_1746_staging_airbyte', 'tb_protocolo_chamado') }} AS t
),

tb_protocolo AS (
    SELECT
        CAST(id_protocolo AS STRING) AS id_protocolo,
        CAST(id_pessoa_fk AS STRING) AS id_pessoa,
        CAST(dt_inicio AS STRING) as dt_inicio,
        CAST(ds_codigo_fk AS STRING) as ds_codigo_fk,
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
        pr.id_pessoa,
        pc.id_protocolo,
        pc.id_protocolo_chamado,
        pr.ds_codigo_fk as numero_protocolo,
        pc.ic_motivo,
        pc._airbyte_extracted_at AS extracted_at,
        SAFE_CAST(DATE_TRUNC(DATE(pr.dt_inicio), day) AS DATE) data_particao
    FROM tb_protocolo_chamado pc
    LEFT JOIN tb_protocolo pr ON pc.id_protocolo = pr.id_protocolo
    LEFT JOIN tb_pessoa p ON pr.id_pessoa = p.id_pessoa
   )

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    cpf, 
    SAFE_CAST(
        REGEXP_REPLACE(id_pessoa, r'\.0$', '') AS STRING
    ) id_pessoa,
    SAFE_CAST(
        REGEXP_REPLACE(id_protocolo, r'\.0$', '') AS STRING
    ) id_protocolo,
    SAFE_CAST(
        REGEXP_REPLACE(id_protocolo_chamado, r'\.0$', '') AS STRING
    ) id_protocolo_chamado,
    SAFE_CAST(
        REGEXP_REPLACE(numero_protocolo, r'\.0$', '') AS STRING
    ) numero_protocolo,
    ic_motivo,
    extracted_at,
    data_particao
FROM source_data AS t