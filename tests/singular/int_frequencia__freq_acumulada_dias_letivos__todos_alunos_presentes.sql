-- este teste checa o calculo da frequencia entre aquilo que é calculado no SGA (model
-- 'gestao_escolar_vw_bi_avlaliacao') contra o calculado no lake (model
-- 'mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos')

{{
    config(
        alias="int_frequencia__freq_acumulada_dias_letivos__todos_alunos_presentes",
        warn_if = ">0",
        error_if = ">5000"
    )
}}



with
    sga as (select * from {{ ref("gestao_escolar_vw_bi_avaliacao") }}),

    lake as (
        select *
        from {{ ref("int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos") }}
    ),

    joined as (

        select
            alu_id,
            sga.ano,
            sga.coc,
            sga.frequencia as freq_sga,
            lake.frequencia_percentual as freq_lake
        from sga
        left join
            lake
            on cast(sga.alu_id as string) = cast(lake.id_aluno as string)
            and cast(sga.ano as string) = cast(lake.ano_calendario as string)
            and cast(sga.coc as string) = cast(lake.id_tipo_calendario as string)
    ),

    final as (
        select distinct alu_id, ano, coc
        from joined
        where cast(ano as string) = '2026' and cast(coc as string) in ('1', '2', '3') and freq_lake is null
    )

select * from final

