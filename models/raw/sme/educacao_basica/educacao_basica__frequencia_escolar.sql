{{
    config(
        alias='frequencia_escolar',
        schema='educacao_basica',
        partition_by={
            "field": "batch",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


with source as (
    select * FROM {{ source('educacao_basica_staging', 'frequencia') }}
)

SELECT
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(esc_id AS STRING), r'\.0$', '') AS STRING) AS id_escola,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_id AS STRING), r'\.0$', '') AS STRING) AS id_turma,
    SAFE_CAST(turma AS STRING) AS turma,
    SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(SAFE_CAST(alu_id AS STRING))
        )
    ), 2,17) as  id_aluno,
    SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(SAFE_CAST(alu_id AS STRING)),
            EXTRACT(YEAR from DATE(batch))
        )
    ), 2,17) as  id_aluno_ano,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(coc AS STRING), r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(datainicio AS DATE) AS data_inicio,
    SAFE_CAST(datafim AS DATE) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(diasletivos AS STRING), r'\.0$', '') AS INT64) AS dias_letivos,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(temposletivos AS STRING), r'\.0$', '') AS INT64) AS tempos_letivos,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(faltasglb AS STRING), r'\.0$', '') AS INT64) AS faltas_global,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(dis_id AS STRING), r'\.0$', '') AS STRING) AS id_disciplina,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(disciplinacodigo AS STRING), r'\.0$', '') AS STRING) AS id_disciplina_ano,
    SAFE_CAST(disciplina AS STRING) AS disciplina,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(faltasdis AS STRING), r'\.0$', '') AS INT64) AS faltas_disciplina,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cargahorariasemanal AS STRING), r'\.0$', '') AS INT64) AS carga_horaria_semanal,
    SAFE_CAST(batch AS DATE) batch,
FROM source
