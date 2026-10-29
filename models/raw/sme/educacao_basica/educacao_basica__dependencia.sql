{{
    config(
        alias='dependencia',
        schema='educacao_basica',
    )
}}

with source as (
    select * FROM {{ source('educacao_basica_staging', 'dependencia') }}
)

SELECT
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(esc_id AS STRING), r'\.0$', '') AS STRING) AS id_escola,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(dep_id AS STRING), r'\.0$', '') AS STRING) AS id_dependencia,
    SAFE_CAST(dependencia AS STRING) AS nome,
    SAFE_CAST(tipo_dep AS STRING) AS tipo,
    SAFE_CAST(dep_aloc_turma AS STRING) AS aloca_turma,
    SAFE_CAST(dep_util_como AS STRING) AS util_como,
    SAFE_CAST(dep_util_como_aloc_turma AS STRING) AS aloca_turma_e_util_como,
    SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(capac_dep AS STRING), r'\.0$', '') AS INT64) AS capacidade,
    SAFE_CAST(area_dep AS INT64) AS area
FROM source
