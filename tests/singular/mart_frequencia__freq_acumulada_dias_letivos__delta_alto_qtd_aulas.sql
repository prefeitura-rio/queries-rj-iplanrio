-- este teste verifica grandes variações entre a quantidade dias letivos registrados
-- no coc atual contra o coc anterior


{{
    config(
        alias="mart_frequencia__freq_acumulada_dias_letivos__delta_alto_qtd_aulas",
        warn_if=">0",
        error_if=">5000",
    )
}}


with
    freq as (
        select *
        from
            {{
                ref(
                    "mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos_pivoted"
                )
            }}
    ),

    test as (
        select
            id_aluno,
            ano_calendario,
            numero_aulas_3,
            numero_aulas_2,
            {{ dbt_utils.safe_divide("numero_aulas_3", "numero_aulas_2") }}
            as taxa_crescimento
        from freq
    )

select *
from test
where ano_calendario = 2026 and taxa_crescimento > 1.3
