{{ config(alias='turma')}}


with source as (
    select * from {{ source('brutos_gestao_escolar_staging', 'turma') }}
),

renamed as (
    SELECT
        SAFE_CAST(SAFE_CAST({{ adapter.quote("ano") }} AS STRING) AS INT64) AS ano,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("area_sala") }} AS STRING) AS FLOAT64) AS area_sala,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("capac_sala") }} AS STRING) AS INT64) AS capacidade_sala,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("curso") }} AS STRING) AS STRING) AS curso,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("dep_id") }} AS STRING) AS STRING) AS id_dependencia,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS STRING) AS id_escola,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("grupamento") }} AS STRING) AS STRING) AS grupamento,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("modalidade") }} AS STRING) AS STRING) AS modalidade,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("nivel_ensino") }} AS STRING) AS STRING) AS nivel_ensino,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("sala") }} AS STRING) AS STRING) AS sala,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("sala_util_como") }} AS STRING) AS STRING) AS sala_util_como,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("tipo_sala") }} AS STRING) AS STRING) AS tipo_sala,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS STRING) AS id_turma,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("turma") }} AS STRING) AS STRING) AS id_turma_escola,
        SAFE_CAST(SAFE_CAST({{ adapter.quote("turno") }} AS STRING) AS STRING) AS turno,
        SAFE_CAST(SAFE_CAST(_prefect_extracted_at AS STRING) AS DATETIME) AS extracted_at,
        SAFE_CAST (
            SAFE_CAST (batch AS STRING) AS DATETIME
        ) AS batch,
    from source
)



SELECT
    *
FROM renamed
