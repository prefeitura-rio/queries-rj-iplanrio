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
                    trim(safe_cast({{ adapter.quote("tur_id") }} as string)), r'\.0$', ''
                ) as string
            ) as tur_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("alu_id") }} as string)), r'\.0$', ''
                ) as string
            ) as alu_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("mtu_id") }} as string)), r'\.0$', ''
                ) as string
            ) as mtu_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_id") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("fav_id") }} as string)), r'\.0$', ''
                ) as string
            ) as fav_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("ava_id") }} as string)), r'\.0$', ''
                ) as string
            ) as ava_id,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_avaliacao") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_avaliacao,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_frequencia") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_frequencia,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_comentarios") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_comentarios,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_relatorio") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_relatorio,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_situacao") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_situacao,
            safe_cast(
                safe_cast({{ adapter.quote("aat_dataCriacao") }} as string) as timestamp
            ) as data_criacao,
            safe_cast(
                safe_cast({{ adapter.quote("aat_dataAlteracao") }} as string) as timestamp
            ) as data_alteracao,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_semProfessor") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_semprofessor,
            safe_cast(
                safe_cast(
                    regexp_replace(
                        trim(safe_cast({{ adapter.quote("aat_numeroFaltas") }} as string)), r'\.0$', ''
                    ) as string
                ) as int64
            ) as aat_numerofaltas,
            safe_cast(
                safe_cast(
                    regexp_replace(
                        trim(safe_cast({{ adapter.quote("aat_numeroAulas") }} as string)), r'\.0$', ''
                    ) as string
                ) as int64
            ) as aat_numeroaulas,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("arq_idRelatorio") }} as string)), r'\.0$', ''
                ) as string
            ) as arq_idrelatorio,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_ausenciasCompensadas") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_ausenciascompensadas,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_avaliacaoAdicional") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_avaliacaoadicional,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_faltoso") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_faltoso,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_frequenciaAcumulada") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_frequenciaacumulada,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_registroexterno") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_registroexterno,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_frequenciaAcumuladaCalculada") }} as string)),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_frequenciaacumuladacalculada,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_naoAvaliado") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_naoavaliado,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_avaliacaoPosConselho") }} as string)), r'\.0$', ''
                ) as string
            ) as aat_avaliacaoposconselho,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_justificativaPosConselho") }} as string)),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_justificativaposconselho,
            safe_cast(
                regexp_replace(
                    trim(safe_cast({{ adapter.quote("aat_frequenciaFinalAjustada") }} as string)),
                    r'\.0$',
                    ''
                ) as string
            ) as aat_frequenciafinalajustada,
            safe_cast(
                safe_cast({{ adapter.quote("_prefect_extracted_at") }} as string) as timestamp
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
