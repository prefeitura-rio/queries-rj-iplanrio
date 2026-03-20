{{
    config(
        alias='dependencia',
        schema='educacao_basica'
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'dependencia') }}
)

SELECT
    {{ clean_and_cast('esc_id', 'string') }} AS id_escola,
    {{ clean_and_cast('dep_id', 'string') }} AS id_dependencia,
    SAFE_CAST(dependencia AS STRING) AS nome,
    SAFE_CAST(tipo_dep AS STRING) AS tipo,
    SAFE_CAST(dep_aloc_turma AS STRING) AS aloca_turma,
    SAFE_CAST(dep_util_como AS STRING) AS util_como,
    SAFE_CAST(dep_util_como_aloc_turma AS STRING) AS aloca_turma_e_util_como,
    {{ clean_and_cast('capac_dep', 'int64') }} AS capacidade,
    SAFE_CAST(area_dep AS INT64) AS area
FROM source
