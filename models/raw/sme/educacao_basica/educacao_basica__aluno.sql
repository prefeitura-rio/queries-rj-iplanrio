{{
    config(
        alias='aluno',
        schema='educacao_basica'
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'aluno') }}
)

SELECT
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_id AS STRING), r'\.0$', '') AS STRING) id_turma,
    SAFE_CAST(turma AS STRING) turma,
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
    SAFE_CAST(matricula AS STRING) matricula,
    SAFE_CAST(situacao AS STRING) situacao,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cod_ult_mov AS STRING), r'\.0$', '') AS STRING) id_utltima_movimentacao,
    SAFE_CAST(ult_movimentacao AS STRING) ultima_movimentacao,
    SAFE_CAST(sexo AS STRING) genero,
    SAFE_CAST(raca_cor AS STRING) raca_cor,
    SAFE_CAST(datanascimento AS DATE) data_nascimento,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(idade_atual AS STRING), r'\.0$', '') AS INT64) idade_atual,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(idade_3112 AS STRING), r'\.0$', '') AS INT64) idade_final_ano,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cod_def AS STRING), r'\.0$', '') AS STRING) id_deficiencia,
    SAFE_CAST(deficiencia AS STRING) deficiencia,
    SAFE_CAST(bairro AS STRING) bairro,
    SAFE_CAST(mora_com_filiacao AS STRING) mora_com_filiacao,
    SAFE_CAST(tipo_transporte AS STRING) tipo_transporte,
    SAFE_CAST(bolsa_familia AS STRING) bolsa_familia,
    SAFE_CAST(cfc AS STRING) cartao_familia_carioca,
    SAFE_CAST(clube_escolar AS STRING) clube_escolar,
    SAFE_CAST(nucleo_artes AS STRING) nucleo_artes,
    SAFE_CAST(mais_educacao AS STRING) mais_educacao,
    SAFE_CAST(territorios_sociais AS STRING) territorio_social,
    SAFE_CAST(up_aval AS STRING) tipo_avaliacao_jovens_adultos
FROM source
