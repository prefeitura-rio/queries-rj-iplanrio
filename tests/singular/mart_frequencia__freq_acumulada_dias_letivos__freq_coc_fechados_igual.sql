-- este teste checa o calculo da frequencia entre aquilo que é calculado no SGA (model
-- 'gestao_escolar_vw_bi_avlaliacao') contra o calculado no lake (model
-- 'mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos')
{{
    config(
        alias="mart_frequencia__freq_acumulada_dias_letivos__freq_coc_fechados_igual",
        warn_if = ">0",
        error_if = ">5000"
    )
}}


with
    sga as (select * from {{ ref("gestao_escolar_vw_bi_avaliacao") }}),

    lake as (
        select *
        from {{ ref("mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos") }}
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
            on sga.alu_id = cast(lake.id_aluno as string)
            and sga.ano = cast(lake.ano_calendario as string)
            and sga.coc = cast(lake.id_tipo_calendario as string)
    ),

    final as (
        select *
        from joined
        where ano = '2026' and coc in ('1', '2') and abs(freq_sga - freq_lake) >= 0.01
    )

select *
from final
