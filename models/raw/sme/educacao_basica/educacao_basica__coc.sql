{{
    config(
        alias='coc',
        schema='educacao_basica',
        partition_by={
            "field": "batch",
            "data_type": "date",
            "granularity": "year",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'coc') }}
)


SELECT
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(ano AS STRING), r'\.0$', '') AS INT64) AS ano,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cre AS STRING), r'\.0$', '') AS STRING) AS id_cre,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_id AS STRING), r'\.0$', '') AS STRING) AS id_turma,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(turma AS STRING), r'\.0$', '') AS STRING) AS id_turma_escola,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(unidade AS STRING), r'\.0$', '') AS STRING) AS id_unidade,
    SAFE_CAST(grupamento AS STRING) AS grupamento,
    SAFE_CAST(turno AS STRING) AS turno,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(coc AS STRING), r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(alunos AS STRING), r'\.0$', '') AS INT64) AS alunos,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(masculinos AS STRING), r'\.0$', '') AS INT64) AS masculino,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(femininos AS STRING), r'\.0$', '') AS INT64) AS feminino,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(def AS STRING), r'\.0$', '') AS INT64) AS deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(masculinos_def AS STRING), r'\.0$', '') AS INT64) AS masculino_deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(femininos_def AS STRING), r'\.0$', '') AS INT64) AS feminino_deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(nao_def AS STRING), r'\.0$', '') AS INT64) AS nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(masculinos_nao_def AS STRING), r'\.0$', '') AS INT64) AS masculino_nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(femininos_nao_def AS STRING), r'\.0$', '') AS INT64) AS feminino_nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(vagas AS STRING), r'\.0$', '') AS INT64) AS vagas, ## valor negativo? superlotacao?
    SAFE_CAST(batch AS DATE) batch

FROM source
