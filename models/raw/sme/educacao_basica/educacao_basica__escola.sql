{{ config(
    alias='escola',
    schema='educacao_basica'
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'escola') }}
)

SELECT
    {{ clean_and_cast('inep', 'string') }} AS id_inep,
    {{ clean_and_cast('esc_id', 'string') }} AS id_escola,
    SAFE_CAST(denominacao AS STRING) AS nome,
    SAFE_CAST(tipo_unidade AS STRING) AS tipo,
    {{ clean_and_cast('designacao', 'string') }} AS id_designacao,
    SAFE_CAST(endereco AS STRING) AS endereco,
    SAFE_CAST(bairro AS STRING) AS bairro,
    SAFE_CAST(cep AS STRING) AS cep,
    SAFE_CAST(email AS STRING) AS email,
    SAFE_CAST(telefone AS STRING) AS telefone,
    SAFE_CAST(direcao AS STRING) AS direcao,
    {{ clean_and_cast('cre', 'string') }} AS id_cre,
    SAFE_CAST(polo AS STRING) AS polo,
    SAFE_CAST(microarea AS STRING) AS micro_area,
    {{ clean_and_cast('sici', 'string') }} AS id_institucional_pcrj,
    SAFE_CAST(salas_recurso AS INT64) AS numero_salas_recurso,
    SAFE_CAST(salas_aula AS INT64) AS numero_salas_aula,
    SAFE_CAST(salas_aula_utilizadas AS INT64) AS numero_salas_utilizadas,
FROM source

