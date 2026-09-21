{{ config(alias="aca_avaliacao", schema="gestao_escolar") }}

with
    source as (
        select * from {{ source("brutos_gestao_escolar_staging", "ACA_Avaliacao") }}
    ),

    renamed as (
        select
            {{ adapter.quote("_airbyte_extracted_at") }} as loaded_at,
            safe_cast(
                {{ adapter.quote("fav_id") }} as string
            ) as {{ adapter.quote("fav_id") }},
            safe_cast(
                {{ adapter.quote("ava_id") }} as string
            ) as {{ adapter.quote("ava_id") }},
            {{ adapter.quote("ava_nome") }},
            {{ adapter.quote("ava_tipo") }},
            safe_cast(
                {{ adapter.quote("tpc_id") }} as string
            ) as {{ adapter.quote("tpc_id") }},
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
            {{
                adapter.quote(
                    "ava_recFinalNotaDisciplinaApenasConceitoGlobalNaoAtingido"
                )
            }},
            {{ adapter.quote("ava_disciplinaObrigatoria") }},
            {{ adapter.quote("ava_exibeNaoAvaliados") }},
            {{ adapter.quote("ava_exibeSemProfessor") }},
            {{ adapter.quote("ava_exibeObservacaoDisciplina") }},
            {{ adapter.quote("ava_exibeObservacaoConselhoPedagogico") }},
            {{ adapter.quote("ava_exibeFrequencia") }},
            {{ adapter.quote("ava_exibeNotaPosConselho") }},
            {{ adapter.quote("ava_conceitoGlobalObrigatorioFrequencia") }}
        from source
    ),

    final as (
        select {{ dbt_utils.generate_surrogate_key(["fav_id", "ava_id"]) }} as id, *
        from renamed
    )

select *
from final
