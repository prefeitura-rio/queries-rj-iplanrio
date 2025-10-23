{{ config(alias='curriculo_periodo') }}

with source as (
    select * from {{ source('sme_brutos_educacao_basica_frequencia_staging_airbyte', 'ACA_CurriculoPeriodo') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        {{ adapter.quote("crp_controletempo") }} AS controle_tempo,
        {{ adapter.quote("crp_dataalteracao") }} AS data_alteracao,
        {{ adapter.quote("crp_datacriacao") }} AS data_criacao,
        {{ adapter.quote("crp_descricao") }} AS descricao_periodo,
        SAFE_CAST({{ adapter.quote("crp_id") }} AS STRING) AS id_periodo_curriculo,
        {{ adapter.quote("crp_idadeidealanofim") }} AS ano_idade_final,
        {{ adapter.quote("crp_idadeidealanoinicio") }} AS ano_idade_inicio,
        {{ adapter.quote("crp_idadeidealmesfim") }} AS mes_idade_final,
        {{ adapter.quote("crp_idadeidealmesinicio") }} AS mes_idade_inicio,
        {{ adapter.quote("crp_ordem") }} AS ordem_periodo,
        {{ adapter.quote("crp_qtdediassemana") }} AS aulas_semana,
        {{ adapter.quote("crp_qtdehorasdia") }} AS horas_dia,
        {{ adapter.quote("crp_qtdeminutosdia") }} AS minutos_dia,
        {{ adapter.quote("crp_qtdetempossemana") }} AS tempos_semana,
        SAFE_CAST({{ adapter.quote("crp_situacao") }} AS STRING) AS id_situacao,
        SAFE_CAST({{ adapter.quote("crr_id") }} AS STRING) AS id_curriculo,
        SAFE_CAST({{ adapter.quote("cur_id") }} AS STRING) AS id_curso,
        SAFE_CAST({{ adapter.quote("mep_id") }} AS STRING) AS id_etapa_mec_inep,
        SAFE_CAST({{ adapter.quote("tcp_id") }} AS STRING) AS id_coc
    from source
)


select * from renamed
