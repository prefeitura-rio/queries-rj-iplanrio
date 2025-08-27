
{{
    config(
        alias='aluno_turma_2025',
        schema='educacao_basica',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "year",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'aluno_turma') }}
)
SELECT
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(REGEXP_REPLACE(tur_id, r'\.0$', '') AS STRING) id_turma,
    TRIM(alu_id) AS id_aluno_original,
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
            SAFE_CAST(TRIM(ano) AS STRING)
        )
    ), 2,17) as  id_aluno_ano,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM source
