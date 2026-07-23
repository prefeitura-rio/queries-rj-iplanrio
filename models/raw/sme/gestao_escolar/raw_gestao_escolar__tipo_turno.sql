{{ config(alias='tipo_turno', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'ACA_TipoTurno') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        {{ adapter.quote("ttn_dataalteracao") }} AS data_alteracao,
        {{ adapter.quote("ttn_datacriacao") }} AS data_criacao,
        SAFE_CAST({{ adapter.quote("ttn_id") }} AS STRING) AS id_tipo_turno,
        {{ adapter.quote("ttn_nome") }} AS tipo_turno,
        SAFE_CAST({{ adapter.quote("ttn_situacao") }} AS STRING) AS id_situacao
    from source
)


select * from renamed
