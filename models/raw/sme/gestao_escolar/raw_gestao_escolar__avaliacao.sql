{{ config(alias='avaliacao', schema='brutos_gestao_escolar') }}

with source as (
    select * from {{ source('brutos_gestao_escolar_staging_airbyte', 'ACA_Avaliacao') }}
),

renamed as (
    select
        {{ adapter.quote("_airbyte_extracted_at") }} AS loaded_at,
        SAFE_CAST({{ adapter.quote("fav_id") }} AS STRING) AS {{ adapter.quote("fav_id") }},
        SAFE_CAST({{ adapter.quote("ava_id") }} AS STRING) AS {{ adapter.quote("ava_id") }},
        {{ adapter.quote("ava_nome") }},
        {{ adapter.quote("ava_tipo") }},
        SAFE_CAST({{ adapter.quote("tpc_id") }} AS STRING) AS {{ adapter.quote("tpc_id") }},
        {{ adapter.quote("ava_ordemPeriodo") }},
        {{ adapter.quote("ava_apareceBoletim") }},
        {{ adapter.quote("ava_situacao") }},
        {{ adapter.quote("ava_dataCriacao") }},
        {{ adapter.quote("ava_dataAlteracao") }},
        {{ adapter.quote("ava_conceitoGlobalObrigatorio") }},
        {{ adapter.quote("ava_baseadaConceitoGlobal") }},
        {{ adapter.quote("ava_baseadaNotaDisciplina") }},
        {{ adapter.quote("ava_baseadaAvaliacaoAdicional") }},
        {{ adapter.quote("ava_mostraBoletimConceitoGlobalNota") }},
        {{ adapter.quote("ava_mostraBoletimConceitoGlobalFrequencia") }},
        {{ adapter.quote("ava_mostraBoletimConceitoGlobalAvaliacaoAdicional") }},
        {{ adapter.quote("ava_mostraBoletimDisciplinaNota") }},
        {{ adapter.quote("ava_mostraBoletimDisciplinaFrequencia") }},
        {{ adapter.quote("ava_recFinalConceitoMaximoAprovacao") }},
        {{ adapter.quote("ava_recFinalConceitoGlobalMinimoNaoAtingido") }},
        {{ adapter.quote("ava_recFinalFrequenciaMinimaFinalNaoAtingida") }},
        {{ adapter.quote("ava_recFinalNotaDisciplinaApenasConceitoGlobalNaoAtingido") }},
        {{ adapter.quote("ava_disciplinaObrigatoria") }},
        {{ adapter.quote("ava_exibeNaoAvaliados") }},
        {{ adapter.quote("ava_exibeSemProfessor") }},
        {{ adapter.quote("ava_exibeObservacaoDisciplina") }},
        {{ adapter.quote("ava_exibeObservacaoConselhoPedagogico") }},
        {{ adapter.quote("ava_exibeFrequencia") }},
        {{ adapter.quote("ava_exibeNotaPosConselho") }},
        {{ adapter.quote("ava_conceitoGlobalObrigatorioFrequencia") }}
    from source
)

select * from renamed