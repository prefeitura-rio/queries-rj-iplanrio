{{
    config(
        alias='movimentacao_escolar',
        schema='educacao_basica',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'movimentacao') }}
)


SELECT
    {{ clean_and_cast('ano', 'int64') }} AS ano,
    {{ clean_and_cast('cre', 'string') }} AS id_cre,
    {{ clean_and_cast('coc', 'string') }} AS id_coc,
    {{ clean_and_cast('unidade', 'string') }} AS id_unidade,
    {{ clean_and_cast('turma', 'string') }} AS id_turma_escola,
    SAFE_CAST(grupamento AS STRING) AS grupamento,
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
    SAFE_CAST(aluno AS STRING) AS matricula,
    SAFE_CAST(sexo AS STRING) AS genero,
    {{ clean_and_cast('cod_def', 'string') }} AS id_deficiencia,
    SAFE_CAST(deficiencia AS STRING) AS deficiencia,
    SAFE_CAST(datanascimento AS DATE) AS data_nascimentoo,
    {{ clean_and_cast('idade_atual', 'int64') }} AS idade_atual,
    {{ clean_and_cast('idade_3112', 'int64') }} AS idade_final_ano,
    SAFE_CAST(data_mov AS DATE) AS data_movimentacao,
    {{ clean_and_cast('cod_mov', 'string') }} AS id_movimentacao,
    SAFE_CAST(movimentacao AS STRING) AS movimentacao,
    SAFE_CAST(mov_ordem AS STRING) AS ordem,
    SAFE_CAST(tipo_mov AS STRING) AS tipo,
    SAFE_CAST(data_particao AS DATE) data_particao,

FROM source
