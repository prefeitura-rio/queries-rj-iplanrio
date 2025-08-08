{{ config(alias='turno', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'ACA_Turno') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("ent_id") }} AS STRING) AS id_entidade,
        {{ adapter.quote("trn_controletempo") }} AS controle_tempo,
        {{ adapter.quote("trn_dataalteracao") }} AS data_alteracao,
        {{ adapter.quote("trn_datacriacao") }} AS data_criacao,
        {{ adapter.quote("trn_descricao") }} AS descricao_turno,
        {{ adapter.quote("trn_horafim") }} AS hora_fim,
        {{ adapter.quote("trn_horainicio") }} AS hora_inicio,
        SAFE_CAST({{ adapter.quote("trn_id") }} AS STRING) AS id_turno,
        {{ adapter.quote("trn_padrao") }} AS turno_padrao,
        {{ adapter.quote("trn_situacao") }} AS situacao,
        SAFE_CAST({{ adapter.quote("ttn_id") }} AS STRING) AS id_tipo_turno
    from source
)

select * from renamed
