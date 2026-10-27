{{ config(alias='confirmacao_efetivacao', schema='gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging', 'CLS_ConfirmacaoEfetivacao') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        {{ adapter.quote("cal_id") }} AS cal_id,
        {{ adapter.quote("cap_id") }} AS cap_id,
        {{ adapter.quote("cne_id") }} AS cne_id,
        {{ adapter.quote("esc_id") }} AS esc_id,
        {{ adapter.quote("uni_id") }} AS uni_id,
        {{ adapter.quote("usu_id") }} AS usu_id,
        {{ adapter.quote("cne_dataHora") }} AS cne_datahora,
        {{ adapter.quote("cne_situacao") }} AS cne_situacao,
        {{ adapter.quote("cne_dataCriacao") }} AS cne_datacriacao,
        {{ adapter.quote("cne_dataAlteracao") }} AS cne_dataalteracao,
        {{ adapter.quote("cne_situacao_recuperacao") }} AS cne_situacao_recuperacao
FROM source
)
select * from renamed
