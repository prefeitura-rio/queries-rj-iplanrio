
{{
    config(
        alias='aluno_turma',
        schema='educacao_basica',
        partition_by={
            "field": "batch",
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
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_id AS STRING), r'\.0$', '') AS STRING) id_turma,
    TRIM(SAFE_CAST(alu_id AS STRING)) AS id_aluno_original,
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
            SAFE_CAST(TRIM(SAFE_CAST(ano AS STRING)) AS STRING)
        )
    ), 2,17) as  id_aluno_ano,
    SAFE_CAST(batch AS DATE) batch,
FROM source
