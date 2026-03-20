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
    {{ clean_and_cast('tur_id', 'string') }} id_turma,
    SAFE_CAST(turma AS STRING) turma,
    TRIM(alu_id)  id_aluno_original,
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
    SAFE_CAST(matricula AS STRING) matricula,
    SAFE_CAST(situacao AS STRING) situacao,
    {{ clean_and_cast('cod_ult_mov', 'string') }} id_utltima_movimentacao,
    SAFE_CAST(ult_movimentacao AS STRING) ultima_movimentacao,
    SAFE_CAST(sexo AS STRING) genero,
    SAFE_CAST(raca_cor AS STRING) raca_cor,
    SAFE_CAST(SAFE.PARSE_DATE('%d/%m/%Y',  datanascimento) AS DATE) data_nascimento,
    {{ clean_and_cast('idade_atual', 'int64') }} idade_atual,
    {{ clean_and_cast('idade_3112', 'int64') }} idade_final_ano,
    {{ clean_and_cast('cod_def', 'string') }} id_deficiencia,
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
