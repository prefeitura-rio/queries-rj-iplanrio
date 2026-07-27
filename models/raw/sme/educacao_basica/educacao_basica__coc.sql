{{
    config(
        alias='coc',
        schema='educacao_basica',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "year",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'coc') }}
)


SELECT
    SAFE_CAST(REGEXP_REPLACE(ano, r'\.0$', '') AS INT64) AS ano,
    SAFE_CAST(REGEXP_REPLACE(cre, r'\.0$', '') AS STRING) AS id_cre,
    SAFE_CAST(REGEXP_REPLACE(tur_id, r'\.0$', '') AS STRING) AS id_turma,
    SAFE_CAST(REGEXP_REPLACE(turma, r'\.0$', '') AS STRING) AS id_turma_escola,
    SAFE_CAST(REGEXP_REPLACE(unidade, r'\.0$', '') AS STRING) AS id_unidade,
    SAFE_CAST(grupamento AS STRING) AS grupamento,
    SAFE_CAST(turno AS STRING) AS turno,
    SAFE_CAST(REGEXP_REPLACE(coc, r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(REGEXP_REPLACE(alunos, r'\.0$', '') AS INT64) AS alunos,
    SAFE_CAST(REGEXP_REPLACE(masculinos, r'\.0$', '') AS INT64) AS masculino,
    SAFE_CAST(REGEXP_REPLACE(femininos, r'\.0$', '') AS INT64) AS feminino,
    SAFE_CAST(REGEXP_REPLACE(def, r'\.0$', '') AS INT64) AS deficiente,
    SAFE_CAST(REGEXP_REPLACE(masculinos_def, r'\.0$', '') AS INT64) AS masculino_deficiente,
    SAFE_CAST(REGEXP_REPLACE(femininos_def, r'\.0$', '') AS INT64) AS feminino_deficiente,
    SAFE_CAST(REGEXP_REPLACE(nao_def, r'\.0$', '') AS INT64) AS nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(masculinos_nao_def, r'\.0$', '') AS INT64) AS masculino_nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(femininos_nao_def, r'\.0$', '') AS INT64) AS feminino_nao_deficiente,
    SAFE_CAST(REGEXP_REPLACE(vagas, r'\.0$', '') AS INT64) AS vagas, ## valor negativo? superlotacao?
    SAFE_CAST(data_particao AS DATE) data_particao

FROM source
