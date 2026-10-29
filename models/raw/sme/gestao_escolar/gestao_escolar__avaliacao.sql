{{
    config(
        alias='avaliacao',
        schema='gestao_escolar',
        partition_by={
            "field": "batch",
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
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_id AS STRING), r'\.0$', '') AS STRING) AS id_turma,
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
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(mtu_id AS STRING), r'\.0$', '') AS STRING) AS id_matricula_turma,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(cur_id AS STRING), r'\.0$', '') AS STRING) AS id_curriculo,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(crp_id AS STRING), r'\.0$', '') AS STRING) AS id_curriculo_periodo,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(coc AS STRING), r'\.0$', '') AS STRING) AS id_coc,
    SAFE_CAST(tpc_nome AS STRING) AS coc,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(tur_codigo AS STRING), r'\.0$', '') AS STRING) AS id_turma_escola,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(frequencia AS STRING), r',', '.') AS FLOAT64) AS frequencia,
    NULLIF(conceito, 'Sem Informação') AS conceito,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(glb AS STRING), r',', '.'), 'Sem Informação') AS nota_fundamental_1,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(mat AS STRING), r',', '.'), 'Sem Informação') AS matematica,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(por AS STRING), r',', '.'), 'Sem Informação') AS portugues,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(cie AS STRING), r',', '.'), 'Sem Informação') AS ciencias,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(geo AS STRING), r',', '.'), 'Sem Informação') AS geografia,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(his AS STRING), r',', '.'), 'Sem Informação') AS historia,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(efi AS STRING), r',', '.'), 'Sem Informação') AS educacao_fisica,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(ing AS STRING), r',', '.'), 'Sem Informação') AS ingles,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(esp AS STRING), r',', '.'), 'Sem Informação') AS espanho,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(fra AS STRING), r',', '.'), 'Sem Informação') AS frances,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(ale AS STRING), r',', '.'), 'Sem Informação') AS alemao,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(avi AS STRING), r',', '.'), 'Sem Informação') AS artes_visuais,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(apl AS STRING), r',', '.'), 'Sem Informação') AS artes_plasticas,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(ace AS STRING), r',', '.'), 'Sem Informação') AS artes_cenicas,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(tea AS STRING), r',', '.'), 'Sem Informação') AS teatro,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(mus AS STRING), r',', '.'), 'Sem Informação') AS musica,
    NULLIF(REGEXP_REPLACE(SAFE_CAST(reuniao_pais AS STRING), r',', '.'), 'Sem Informação') AS reuniao_pais,
    SAFE_CAST(batch AS DATE) batch,
FROM source

