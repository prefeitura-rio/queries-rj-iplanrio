{{ config(
    alias='escola',
    schema='educacao_basica'
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'escola') }}
)

SELECT
    SAFE_CAST(REGEXP_REPLACE(inep, r'\.0$', '') AS STRING) AS id_inep,
    SAFE_CAST(REGEXP_REPLACE(esc_id, r'\.0$', '') AS STRING) AS id_escola,
    SAFE_CAST(denominacao AS STRING) AS nome,
    SAFE_CAST(tipo_unidade AS STRING) AS tipo,
    SAFE_CAST(REGEXP_REPLACE(designacao, r'\.0$', '') AS STRING) AS id_designacao,
    SAFE_CAST(endereco AS STRING) AS endereco,
    SAFE_CAST(bairro AS STRING) AS bairro,
    SAFE_CAST(cep AS STRING) AS cep,
    SAFE_CAST(email AS STRING) AS email,
    SAFE_CAST(telefone AS STRING) AS telefone,
    SAFE_CAST(direcao AS STRING) AS direcao,
    SAFE_CAST(REGEXP_REPLACE(cre, r'\.0$', '') AS STRING) AS id_cre,
    SAFE_CAST(polo AS STRING) AS polo,
    SAFE_CAST(microarea AS STRING) AS micro_area,
    SAFE_CAST(REGEXP_REPLACE(sici, r'\.0$', '') AS STRING) AS id_institucional_pcrj,
    SAFE_CAST(salas_recurso AS INT64) AS numero_salas_recurso,
    SAFE_CAST(salas_aula AS INT64) AS numero_salas_aula,
    SAFE_CAST(salas_aula_utilizadas AS INT64) AS numero_salas_utilizadas,
FROM source

