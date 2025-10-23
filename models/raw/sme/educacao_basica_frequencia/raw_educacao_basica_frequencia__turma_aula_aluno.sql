
{{
    config(
        alias='turma_aula_aluno',
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={
            "field": "data_alteracao",
            "data_type": "timestamp",
            "granularity": "year"
        },
        unique_key=['id_aluno', 'id_matricula_disciplina','id_matricula_turma','id_aula_disciplina','id_disciplina_turma'],
        cluster_by=['id_aluno']
    )
}}

with source as (
    select * from {{ source('sme_brutos_educacao_basica_frequencia_staging_prefect', 'CLS_TurmaAulaAluno') }}
    {% if is_incremental() %}
      where data_particao in (CAST(current_date AS STRING), CAST(date_sub(current_date, interval 1 day) AS STRING))
    {% endif %}
),
renamed as (
    select
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("alu_id") }}), r'\.0$', '') AS STRING) AS id_aluno,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("mtd_id") }}), r'\.0$', '') AS STRING) AS id_matricula_disciplina,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("mtu_id") }}), r'\.0$', '') AS STRING) AS id_matricula_turma,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("taa_anotacao") }}), r'\.0$', '') AS STRING) AS anotacao,
        SAFE_CAST({{ adapter.quote("taa_dataalteracao") }} AS TIMESTAMP) AS data_alteracao,
        SAFE_CAST({{ adapter.quote("taa_datacriacao") }} AS TIMESTAMP) AS data_criacao,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("taa_frequencia") }}), r'\.0$', '') AS INT64) AS faltas_disciplina_dia,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("taa_frequenciabitmap") }}), r'\.0$', '') AS STRING) AS frequencia_tempo,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("taa_situacao") }}), r'\.0$', '') AS STRING) AS id_situacao,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("tau_id") }}), r'\.0$', '') AS STRING) AS id_aula_disciplina,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("tud_id") }}), r'\.0$', '') AS STRING) AS id_disciplina_turma,
        SAFE_CAST(REGEXP_REPLACE(TRIM({{ adapter.quote("usu_iddocentealteracao") }}), r'\.0$', '') AS STRING) AS usuario_alteracao,
    from source
), dedup AS (
    select *,
        row_number() over (
            partition by id_aluno, id_matricula_disciplina, id_matricula_turma, id_aula_disciplina, id_disciplina_turma
            order by data_alteracao desc
        ) as row_num
    from renamed

)
SELECT
    id_aluno,
    id_matricula_disciplina,
    id_matricula_turma,
    anotacao,
    data_alteracao,
    data_criacao,
    faltas_disciplina_dia,
    frequencia_tempo,
    id_situacao,
    id_aula_disciplina,
    id_disciplina_turma,
    usuario_alteracao
FROM dedup
WHERE row_num = 1