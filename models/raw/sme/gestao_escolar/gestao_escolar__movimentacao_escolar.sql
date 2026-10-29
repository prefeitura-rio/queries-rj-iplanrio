{{
    config(
        alias='movimentacao_escolar',
        schema='gestao_escolar',
        partition_by={
            "field": "batch",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'movimentacao') }}
)


SELECT
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(ano AS STRING), r'\.0$', '') AS INT64) AS ano,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cre AS STRING), r'\.0$', '') AS STRING) AS id_cre,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(coc AS STRING), r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(unidade AS STRING), r'\.0$', '') AS STRING) AS id_unidade,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(turma AS STRING), r'\.0$', '') AS STRING) AS id_turma_escola,
    SAFE_CAST(grupamento AS STRING) AS grupamento,
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
    SAFE_CAST(aluno AS STRING) AS matricula,
    SAFE_CAST(sexo AS STRING) AS genero,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cod_def AS STRING), r'\.0$', '') AS STRING) AS id_deficiencia,
    SAFE_CAST(deficiencia AS STRING) AS deficiencia,
    SAFE_CAST(datanascimento AS DATE) AS data_nascimentoo,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(idade_atual AS STRING), r'\.0$', '') AS INT64) AS idade_atual,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(idade_3112 AS STRING), r'\.0$', '') AS INT64) AS idade_final_ano,
    SAFE_CAST(data_mov AS DATE) AS data_movimentacao,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cod_mov AS STRING), r'\.0$', '') AS STRING) AS id_movimentacao,
    SAFE_CAST(movimentacao AS STRING) AS movimentacao,
    SAFE_CAST(mov_ordem AS STRING) AS ordem,
    SAFE_CAST(tipo_mov AS STRING) AS tipo,
    SAFE_CAST(batch AS DATE) batch,

FROM source
