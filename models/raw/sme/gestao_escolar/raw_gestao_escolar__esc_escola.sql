{{ config(alias='esc_escola') }}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'ESC_Escola') }}
  ),
renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("cid_id") }} AS STRING) AS id_cid,
        SAFE_CAST({{ adapter.quote("ent_id") }} AS STRING) AS id_ent,
        SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS id_esc,
        SAFE_CAST({{ adapter.quote("tre_id") }} AS STRING) AS id_tre,
        SAFE_CAST({{ adapter.quote("uad_id") }} AS STRING) AS id_uad,
        {{ adapter.quote("esc_nome") }} AS esc_nome,
        {{ adapter.quote("esc_codigo") }} AS esc_codigo,
        {{ adapter.quote("esc_situacao") }} AS esc_situacao,
        {{ adapter.quote("esc_atoCriacao") }} AS esc_atoCriacao,
        {{ adapter.quote("esc_autorizada") }} AS esc_autorizada,
        {{ adapter.quote("esc_codigoInep") }} AS esc_codigoInep,
        {{ adapter.quote("esc_fundoVerso") }} AS esc_fundoVerso,
        {{ adapter.quote("esc_dataCriacao") }} AS esc_dataCriacao,
        {{ adapter.quote("esc_dataAlteracao") }} AS esc_dataAlteracao,
        {{ adapter.quote("esc_microareaCampo") }} AS esc_microareaCampo,
        {{ adapter.quote("esc_controleSistema") }} AS esc_controleSistema,
        {{ adapter.quote("esc_funcionamentoFim") }} AS esc_funcionamentoFim,
        SAFE_CAST({{ adapter.quote("uad_idSuperiorGestao") }} AS STRING) AS id_uadSuperiorGestao,
        {{ adapter.quote("esc_funcionamentoInicio") }} AS esc_funcionamentoInicio,
        {{ adapter.quote("esc_codigoNumeroMatricula") }} AS esc_codigoNumeroMatricula,
        {{ adapter.quote("esc_dataPublicacaoDiarioOficial") }} AS esc_dataPublicacaoDiarioOficial
    from source
)

  select * from renamed

