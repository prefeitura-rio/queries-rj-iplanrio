{{
    config(
        alias='turma_disciplina',
        schema='brutos_gestao_escolar',
        materialized='incremental',
        unique_key=['id_disciplina_turma']
    )
}}

with source as (
    select * from {{ source('sme_brutos_gestao_escolar_staging_airbyte', 'TUR_TurmaDisciplina') }}
    {% if is_incremental() %}
      where _airbyte_extracted_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        {{ adapter.quote("tud_aulaforaperiodonormal") }} AS aula_fora_periodo_normal,
        {{ adapter.quote("tud_cargahorariasemanal") }} AS carga_hora_semanal,
        SAFE_CAST({{ adapter.quote("tud_codigo") }} AS STRING) AS id_disciplina,
        {{ adapter.quote("tud_dataalteracao") }} AS data_alteracao,
        {{ adapter.quote("tud_datacriacao") }} AS data_criacao,
        {{ adapter.quote("tud_datafim") }} AS data_fim,
        {{ adapter.quote("tud_datainicio") }} AS data_inicio,
        {{ adapter.quote("tud_disciplinaespecial") }} AS disciplina_especial,
        SAFE_CAST({{ adapter.quote("tud_duracao") }} AS STRING) id_duracao,
        {{ adapter.quote("tud_global") }} AS global,
        SAFE_CAST({{ adapter.quote("tud_id") }} AS STRING) id_disciplina_turma,
        {{ adapter.quote("tud_minimomatriculados") }} AS minimo_matriculados,
        SAFE_CAST({{ adapter.quote("tud_modo") }} AS STRING) AS id_modo,
        {{ adapter.quote("tud_multiseriado") }} AS multiseriado,
        {{ adapter.quote("tud_nome") }} AS nome_disciplina,
        SAFE_CAST({{ adapter.quote("tud_situacao") }} AS STRING) AS id_situacao,
        SAFE_CAST({{ adapter.quote("tud_tipo") }} AS STRING) AS id_tipo,
        {{ adapter.quote("tud_vagas") }} AS numero_vagas
    from source
)

select * from renamed
