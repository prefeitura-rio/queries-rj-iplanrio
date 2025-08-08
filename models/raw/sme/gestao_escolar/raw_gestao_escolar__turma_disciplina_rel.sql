{{ config(
        alias='turma_disciplina_rel',
        schema='brutos_gestao_escolar',
        materialized='incremental',
        unique_key=['id_disciplina', 'id_turma'])
    }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'TUR_TurmaRelTurmaDisciplina') }}
    {% if is_incremental() %}
      where _airbyte_extracted_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("tud_id") }} AS STRING) AS id_disciplina,
        SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS id_turma
    from source
)

select * from renamed