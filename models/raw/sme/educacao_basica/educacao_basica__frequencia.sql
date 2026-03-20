{{
    config(
        alias='frequencia_escolar',
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
    {{ clean_and_cast('esc_id', 'string') }} AS id_escola,
    {{ clean_and_cast('tur_id', 'string') }} AS id_turma,
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
    {{ clean_and_cast('coc', 'string') }} AS id_coc,
    SAFE_CAST(datainicio AS DATE) AS data_inicio,
    SAFE_CAST(datafim AS DATE) AS data_fim,
    {{ clean_and_cast('diasletivos', 'int64') }} AS dias_letivos,
    {{ clean_and_cast('temposletivos', 'int64') }} AS tempos_letivos,
    {{ clean_and_cast('faltasglb', 'int64') }} AS faltas_global,
    {{ clean_and_cast('dis_id', 'string') }} AS id_disciplina,
    {{ clean_and_cast('disciplinacodigo', 'string') }} AS id_disciplina_ano,
    SAFE_CAST(disciplina AS STRING) AS disciplina,
    {{ clean_and_cast('faltasdis', 'int64') }} AS faltas_disciplina,
    {{ clean_and_cast('cargahorariasemanal', 'int64') }} AS carga_horaria_semanal,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM source
