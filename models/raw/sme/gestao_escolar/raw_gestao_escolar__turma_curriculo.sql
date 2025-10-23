{{ config(alias='turma_curriculo') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'TUR_TurmaCurriculo') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("crp_id") }} AS STRING) AS id_periodo_curriculo,
        SAFE_CAST({{ adapter.quote("crr_id") }} AS STRING) AS id_curriculo,
        SAFE_CAST({{ adapter.quote("cur_id") }} AS STRING) AS id_curso,
        {{ adapter.quote("tcr_dataalteracao") }} AS data_alteracao,
        {{ adapter.quote("tcr_datacriacao") }} AS data_criacao,
        {{ adapter.quote("tcr_prioridade") }} AS prioridade_curriculo,
        SAFE_CAST({{ adapter.quote("tcr_situacao") }} AS STRING) AS id_situacao,
        SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS id_turma
    from source
)

select * from renamed