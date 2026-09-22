with
    source as (
        select * except (id)
        from {{ ref("mart_frequencia__vw_alunos_frequencia_acumulada_dias_letivos") }}
    ),

    pivoted as (
        select *
        from
            source pivot (
                sum(numero_aulas) as numero_aulas,
                sum(numero_faltas) as numero_faltas,
                avg(frequencia_percentual) as frequencia for id_tipo_calendario
                in (1, 2, 3, 4)
            )
    )

select *
from pivoted
