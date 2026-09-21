{{
    config(
        alias="aluno_avaliacao_turma",
        schema="gestao_escolar",
        partition_by={
            "field": "data_alteracao",
            "data_type": "timestamp",
            "granularity": "year",
        },
        unique_key=["tur_id", "alu_id", "mtu_id", "aat_id"],
        cluster_by=["alu_id"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_gestao_escolar_staging", "CLS_AlunoAvaliacaoTurma") }}

    ),
    renamed as (
        select
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("tur_id") }}), r'\.0$', ''
                ) as string
            ) as tur_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("alu_id") }}), r'\.0$', ''
                ) as string
            ) as alu_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("mtu_id") }}), r'\.0$', ''
                ) as string
            ) as mtu_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_id") }}), r'\.0$', ''
                ) as string
            ) as aat_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("fav_id") }}), r'\.0$', ''
                ) as string
            ) as fav_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("ava_id") }}), r'\.0$', ''
                ) as string
            ) as ava_id,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_avaliacao") }}), r'\.0$', ''
                ) as string
            ) as aat_avaliacao,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_frequencia") }}), r'\.0$', ''
                ) as string
            ) as aat_frequencia,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_comentarios") }}), r'\.0$', ''
                ) as string
            ) as aat_comentarios,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_relatorio") }}), r'\.0$', ''
                ) as string
            ) as aat_relatorio,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_situacao") }}), r'\.0$', ''
                ) as string
            ) as aat_situacao,
            safe_cast(
                {{ adapter.quote("aat_dataCriacao") }} as timestamp
            ) as data_criacao,
            safe_cast(
                {{ adapter.quote("aat_dataAlteracao") }} as timestamp
            ) as data_alteracao,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_semProfessor") }}), r'\.0$', ''
                ) as string
            ) as aat_semprofessor,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_numeroFaltas") }}), r'\.0$', ''
                ) as int64
            ) as aat_numerofaltas,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_numeroAulas") }}), r'\.0$', ''
                ) as int64
            ) as aat_numeroaulas,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("arq_idRelatorio") }}), r'\.0$', ''
                ) as string
            ) as arq_idrelatorio,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_ausenciasCompensadas") }}), r'\.0$', ''
                ) as string
            ) as aat_ausenciascompensadas,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_avaliacaoAdicional") }}), r'\.0$', ''
                ) as string
            ) as aat_avaliacaoadicional,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_faltoso") }}), r'\.0$', ''
                ) as string
            ) as aat_faltoso,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_frequenciaAcumulada") }}), r'\.0$', ''
                ) as string
            ) as aat_frequenciaacumulada,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_registroexterno") }}), r'\.0$', ''
                ) as string
            ) as aat_registroexterno,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_frequenciaAcumuladaCalculada") }}),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_frequenciaacumuladacalculada,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_naoAvaliado") }}), r'\.0$', ''
                ) as string
            ) as aat_naoavaliado,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_avaliacaoPosConselho") }}), r'\.0$', ''
                ) as string
            ) as aat_avaliacaoposconselho,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_justificativaPosConselho") }}),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_justificativaposconselho,
            safe_cast(
                regexp_replace(
                    trim({{ adapter.quote("aat_frequenciaFinalAjustada") }}),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_frequenciafinalajustada,
            safe_cast(
                {{ adapter.quote("_prefect_extracted_at") }} as timestamp
            ) as loaded_at
        from source
    ),
    dedup as (
        select
            *
            from renamed
            qualify row_number() over (
                partition by tur_id, alu_id, mtu_id, aat_id order by data_alteracao desc
            ) = 1
    ),

    final as (select 
    
        {{dbt_utils.generate_surrogate_key(['tur_id', 'alu_id', 'mtu_id', 'aat_id'])}} as id,
        *
        from dedup
    )


select *
from final
