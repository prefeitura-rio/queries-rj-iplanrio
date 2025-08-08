{{ config(alias='formato_avaliacao', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'ACA_FormatoAvaliacao') }}
),
renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("ent_id") }} AS STRING) AS id_entidade,
        SAFE_CAST({{ adapter.quote("esc_id") }} AS STRING) AS id_escola,
        SAFE_CAST({{ adapter.quote("fav_id") }} AS STRING) AS id_formato_avaliacao,
        SAFE_CAST({{ adapter.quote("uni_id") }} AS STRING) AS id_unidade_escola,
        {{ adapter.quote("fav_nome") }} AS nome_avaliacao,
        {{ adapter.quote("fav_tipo") }} AS tipo_avaliacao,
        {{ adapter.quote("fav_padrao") }} AS formato_avaliacao,
        {{ adapter.quote("fav_situacao") }} AS situacao_registro,
        {{ adapter.quote("fav_dataCriacao") }} AS data_criacao,
        {{ adapter.quote("fav_dataAlteracao") }} AS data_altercao,
        {{ adapter.quote("esa_idConceitoGlobal") }} AS conceito_global,
        {{ adapter.quote("fav_tipoApuracaoFrequencia") }} AS tipo_frequencia_apurada,
        {{ adapter.quote("fav_tipoLancamentoFrequencia") }} AS tipo_frequencia,
        {{ adapter.quote("percentualMinimoFrequencia") }} AS percentual_frequencia,
        {{ adapter.quote("qtdeMaxDisciplinasProgressaoParcial") }} AS maximo_progressao,
        {{ adapter.quote("tipoProgressaoParcial") }} AS tipo_progressao,
        {{ adapter.quote("valorMinimoAprovacaoConceitoGlobal") }} AS valor_aprovacao_global,
        {{ adapter.quote("valorMinimoAprovacaoPorDisciplina") }} AS valor_aprovacao_disciplina,
        {{ adapter.quote("valorMinimoProgressaoParcialPorDisciplina") }} AS valor_progressao
    from source
)
select * from renamed
