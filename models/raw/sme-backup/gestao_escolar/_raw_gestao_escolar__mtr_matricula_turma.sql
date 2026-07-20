{{ config(
        alias='mtr_matricula_turma',
        schema='brutos_gestao_escolar'

    )}}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'MTR_MatriculaTurma') }}
    
),
renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("alu_id") }} AS STRING) AS alu_id,
        SAFE_CAST({{ adapter.quote("mtu_id") }} AS STRING) AS mtu_id,
        SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS tur_id,
        SAFE_CAST({{ adapter.quote("cur_id") }} AS STRING) AS cur_id,
        SAFE_CAST({{ adapter.quote("crr_id") }} AS STRING) AS crr_id,
        SAFE_CAST({{ adapter.quote("crp_id") }} AS STRING) AS crp_id,
        {{ adapter.quote("mtu_dataMatricula") }} AS mtu_dataMatricula,
        {{ adapter.quote("mtu_avaliacao") }} AS mtu_avaliacao,
        {{ adapter.quote("mtu_frequencia") }} AS mtu_frequencia,
        {{ adapter.quote("mtu_relatorio") }} AS mtu_relatorio,
        {{ adapter.quote("mtu_resultado") }} AS mtu_resultado,
        {{ adapter.quote("mtu_dataSaida") }} AS mtu_dataSaida,
        {{ adapter.quote("mtu_situacao") }} AS mtu_situacao,
        {{ adapter.quote("mtu_dataCriacao") }} AS mtu_dataCriacao,
        {{ adapter.quote("mtu_dataAlteracao") }} AS mtu_dataAlteracao,
        {{ adapter.quote("mtu_numeroChamada") }} AS mtu_numeroChamada,
        SAFE_CAST({{ adapter.quote("alc_id") }} AS STRING) AS alc_id,
        SAFE_CAST({{ adapter.quote("usu_idResultado") }} AS STRING) AS usu_idResultado
    from source
)

select * from renamed