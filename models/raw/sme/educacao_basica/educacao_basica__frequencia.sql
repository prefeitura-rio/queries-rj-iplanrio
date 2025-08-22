{{
    config(
        alias='frequencia',
        schema='educacao_basica',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


with source as (
    select * FROM {{ source('educacao_basica_staging', 'frequencia') }}
)

SELECT
    SAFE_CAST(REGEXP_REPLACE(esc_id, r'\.0$', '') AS STRING) AS id_escola,
    SAFE_CAST(REGEXP_REPLACE(tur_id, r'\.0$', '') AS STRING) AS id_turma,
    SAFE_CAST(turma AS STRING) AS turma,
    SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(alu_id)
        )
    ), 2,17) as  id_aluno,
    SUBSTR(SHA256(
        CONCAT(
            '{{ var("HASH_SEED") }}',
            TRIM(alu_id),
            SAFE_CAST(ano_particao AS STRING)
        )
    ), 2,17) as  id_aluno_ano,
    SAFE_CAST(REGEXP_REPLACE(coc, r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(datainicio AS DATE) AS data_inicio,
    SAFE_CAST(datafim AS DATE) AS data_fim,
    SAFE_CAST(REGEXP_REPLACE(diasletivos, r'\.0$', '') AS INT64) AS dias_letivos,
    SAFE_CAST(REGEXP_REPLACE(temposletivos, r'\.0$', '') AS INT64) AS tempos_letivos,
    SAFE_CAST(REGEXP_REPLACE(faltasglb, r'\.0$', '') AS INT64) AS faltas_global,
    SAFE_CAST(REGEXP_REPLACE(dis_id, r'\.0$', '') AS STRING) AS id_disciplina,
    SAFE_CAST(REGEXP_REPLACE(disciplinacodigo, r'\.0$', '') AS STRING) AS id_disciplina_ano,
    SAFE_CAST(disciplina AS STRING) AS disciplina,
    SAFE_CAST(REGEXP_REPLACE(faltasdis, r'\.0$', '') AS INT64) AS faltas_disciplina,
    SAFE_CAST(REGEXP_REPLACE(cargahorariasemanal, r'\.0$', '') AS INT64) AS carga_horaria_semanal,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM source
