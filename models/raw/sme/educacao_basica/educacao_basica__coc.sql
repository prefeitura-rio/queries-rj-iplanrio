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
    {{ clean_and_cast('ano', 'int64') }} AS ano,
    {{ clean_and_cast('cre', 'string') }} AS id_cre,
    {{ clean_and_cast('tur_id', 'string') }} AS id_turma,
    {{ clean_and_cast('turma', 'string') }} AS id_turma_escola,
    {{ clean_and_cast('unidade', 'string') }} AS id_unidade,
    SAFE_CAST(grupamento AS STRING) AS grupamento,
    SAFE_CAST(turno AS STRING) AS turno,
    {{ clean_and_cast('coc', 'string') }} AS id_coc,
    {{ clean_and_cast('alunos', 'int64') }} AS alunos,
    {{ clean_and_cast('masculinos', 'int64') }} AS masculino,
    {{ clean_and_cast('femininos', 'int64') }} AS feminino,
    {{ clean_and_cast('def', 'int64') }} AS deficiente,
    {{ clean_and_cast('masculinos_def', 'int64') }} AS masculino_deficiente,
    {{ clean_and_cast('femininos_def', 'int64') }} AS feminino_deficiente,
    {{ clean_and_cast('nao_def', 'int64') }} AS nao_deficiente,
    {{ clean_and_cast('masculinos_nao_def', 'int64') }} AS masculino_nao_deficiente,
    {{ clean_and_cast('femininos_nao_def', 'int64') }} AS feminino_nao_deficiente,
    {{ clean_and_cast('vagas', 'int64') }} AS vagas, ## valor negativo? superlotacao?
    SAFE_CAST(data_particao AS DATE) data_particao

FROM source
