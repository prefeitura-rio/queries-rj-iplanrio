{{
    config(
        alias='avaliacao',
        schema='educacao_basica',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "year",
        }
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'avaliacao') }}
)


SELECT
    SAFE_CAST(ano AS INT64) AS ano,
    SAFE_CAST(REGEXP_REPLACE(tur_id, r'\.0$', '') AS STRING) AS id_turma,
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
    SAFE_CAST(REGEXP_REPLACE(mtu_id, r'\.0$', '') AS STRING) AS id_matricula_turma,
    SAFE_CAST(REGEXP_REPLACE(cur_id, r'\.0$', '') AS STRING) AS id_curriculo,
    SAFE_CAST(REGEXP_REPLACE(crp_id, r'\.0$', '') AS STRING) AS id_curriculo_periodo,
    SAFE_CAST(REGEXP_REPLACE(coc, r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(tpc_nome AS STRING) AS coc,
    SAFE_CAST(REGEXP_REPLACE(tur_codigo, r'\.0$', '') AS STRING) AS id_turma_escola,
    SAFE_CAST(REGEXP_REPLACE(frequencia, r',', '.') AS FLOAT64) AS frequencia,
    NULLIF(conceito, 'Sem Informação') AS conceito,
    NULLIF(REGEXP_REPLACE(glb, r',', '.'), 'Sem Informação') AS nota_fundamental_1,
    NULLIF(REGEXP_REPLACE(mat, r',', '.'), 'Sem Informação') AS matematica,
    NULLIF(REGEXP_REPLACE(por, r',', '.'), 'Sem Informação') AS portugues,
    NULLIF(REGEXP_REPLACE(cie, r',', '.'), 'Sem Informação') AS ciencias,
    NULLIF(REGEXP_REPLACE(geo, r',', '.'), 'Sem Informação') AS geografia,
    NULLIF(REGEXP_REPLACE(his, r',', '.'), 'Sem Informação') AS historia,
    NULLIF(REGEXP_REPLACE(efi, r',', '.'), 'Sem Informação') AS educacao_fisica,
    NULLIF(REGEXP_REPLACE(ing, r',', '.'), 'Sem Informação') AS ingles,
    NULLIF(REGEXP_REPLACE(esp, r',', '.'), 'Sem Informação') AS espanho,
    NULLIF(REGEXP_REPLACE(fra, r',', '.'), 'Sem Informação') AS frances,
    NULLIF(REGEXP_REPLACE(ale, r',', '.'), 'Sem Informação') AS alemao,
    NULLIF(REGEXP_REPLACE(avi, r',', '.'), 'Sem Informação') AS artes_visuais,
    NULLIF(REGEXP_REPLACE(apl, r',', '.'), 'Sem Informação') AS artes_plasticas,
    NULLIF(REGEXP_REPLACE(ace, r',', '.'), 'Sem Informação') AS artes_cenicas,
    NULLIF(REGEXP_REPLACE(tea, r',', '.'), 'Sem Informação') AS teatro,
    NULLIF(REGEXP_REPLACE(mus, r',', '.'), 'Sem Informação') AS musica,
    NULLIF(REGEXP_REPLACE(reuniao_pais, r',', '.'), 'Sem Informação') AS reuniao_pais,
    SAFE_CAST(data_particao AS DATE) data_particao,
FROM source

