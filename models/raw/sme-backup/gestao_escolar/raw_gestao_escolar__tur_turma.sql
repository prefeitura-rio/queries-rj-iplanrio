{{ config(alias='tur_turma', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'TUR_Turma') }}
  ),
renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("cal_id") }} AS STRING) AS cal_id,
        SAFE_CAST({{ adapter.quote("dep_id") }} AS STRING) AS dep_id,
        SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS esc_id,
        SAFE_CAST({{ adapter.quote("fav_id") }} AS STRING) AS fav_id,
        SAFE_CAST({{ adapter.quote("prd_id") }} AS STRING) AS prd_id,
        SAFE_CAST({{ adapter.quote("trn_id") }} AS STRING) AS trn_id,
        SAFE_CAST({{ adapter.quote("tur_id") }} AS STRING) AS tur_id,
        SAFE_CAST({{ adapter.quote("uni_id") }} AS STRING) AS uni_id,
        {{ adapter.quote("tur_tipo") }} AS tur_tipo,
        {{ adapter.quote("tur_vagas") }} AS tur_vagas,
        {{ adapter.quote("tur_codigo") }} AS tur_codigo,
        {{ adapter.quote("tur_duracao") }} AS tur_duracao,
        {{ adapter.quote("tur_situacao") }} AS tur_situacao,
        {{ adapter.quote("tur_descricao") }} AS tur_descricao,
        {{ adapter.quote("tur_codigoInep") }} AS tur_codigoInep,
        {{ adapter.quote("tur_observacao") }} AS tur_observacao,
        {{ adapter.quote("tur_seqChamada") }} AS tur_seqChamada,
        {{ adapter.quote("tur_dataCriacao") }} AS tur_dataCriacao,
        {{ adapter.quote("tur_dataAlteracao") }} AS tur_dataAlteracao,
        {{ adapter.quote("tur_dataEncerramento") }} AS tur_dataEncerramento,
        {{ adapter.quote("tur_participaRodizio") }} AS tur_participaRodizio,
        {{ adapter.quote("tur_flag_tem_acrescimo") }} AS tur_flag_tem_acrescimo,
        {{ adapter.quote("tur_minimoMatriculados") }} AS tur_minimoMatriculados,
        {{ adapter.quote("tur_docenteEspecialista") }} AS tur_docenteEspecialista
    from source
)

  select * from renamed