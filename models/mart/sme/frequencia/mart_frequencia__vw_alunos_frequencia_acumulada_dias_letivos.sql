{{
    config(
        alias="frequencia_acumulada_dias_letivos",
    )
}}

-- Nome frequencia acumulada dias letivos
with
    source as (
        select *
        from {{ ref("int_frequencia__vw_alunos_frequencia_acumulada_dias_letivos") }}
    )

select *
from source
