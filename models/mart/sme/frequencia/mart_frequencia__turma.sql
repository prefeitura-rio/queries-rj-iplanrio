{{ config(alias='turma')}}


with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_prefect', 'turma') }}
),

renamed as (
    SELECT
        SAFE_CAST({{ adapter.quote("ano") }} AS INT64) AS ano,
        SAFE_CAST({{ adapter.quote("area_sala") }} AS FLOAT64) AS area_sala,
        SAFE_CAST({{ adapter.quote("capac_sala") }} AS INT64) AS capacidade_sala,
        SAFE_CAST({{ adapter.quote("curso") }} AS STRING) AS curso,
        SAFE_CAST({{ adapter.quote("dep_id") }} AS STRING) AS id_dependencia,
        SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS id_escola,
        SAFE_CAST({{ adapter.quote("grupamento") }} AS STRING) AS grupamento,
        SAFE_CAST({{ adapter.quote("modalidade") }} AS STRING) AS modalidade,
        SAFE_CAST({{ adapter.quote("nivel_ensino") }} AS STRING) AS nivel_ensino,
        SAFE_CAST({{ adapter.quote("sala") }} AS STRING) AS sala,
        SAFE_CAST({{ adapter.quote("sala_util_como") }} AS STRING) AS sala_util_como,
        SAFE_CAST({{ adapter.quote("tipo_sala") }} AS STRING) AS tipo_sala,
        SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS id_turma,
        SAFE_CAST({{ adapter.quote("turma") }} AS STRING) AS id_turma_escola,
        SAFE_CAST({{ adapter.quote("turno") }} AS STRING) AS turno
    from source
)



SELECT
    *
FROM renamed
