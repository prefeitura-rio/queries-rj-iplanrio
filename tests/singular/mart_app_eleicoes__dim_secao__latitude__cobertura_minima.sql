-- retorna anos em que mais de 5% das seções estão sem coordenada
{{
    config(
        alias="mart_app_eleicoes__dim_secao__latitude__cobertura_minima",
        severity="warn",
    )
}}

select
    ano,
    count(*) as secoes,
    countif(latitude is null) as secoes_sem_coordenada,
    safe_divide(countif(latitude is null), count(*)) as proporcao_sem_coordenada
from {{ ref("mart_app_eleicoes__dim_secao") }}
group by ano
having safe_divide(countif(latitude is null), count(*)) > 0.05
