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
    {{ clean_and_cast('id_chamado', 'string') }} id_chamado,
    cpf, 
    {{ clean_and_cast('id_pessoa', 'string') }} id_pessoa,
    {{ clean_and_cast('id_protocolo', 'string') }} id_protocolo,
    {{ clean_and_cast('id_protocolo_chamado', 'string') }} id_protocolo_chamado,
    {{ clean_and_cast('numero_protocolo', 'string') }} numero_protocolo,
    ic_motivo,
    extracted_at,
    data_particao
FROM source_data AS t