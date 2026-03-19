{{
    config(
        alias='matricula',
        materialized="table",
        tags=["raw", "ergon", "matricula"],
        description="Tabela que armazena as matrículas de cada servidor municipal."
    )
}}

SELECT
    a.id_funcionario,
    a.id_matricula
FROM {{ ref('raw_recursos_humanos_ergon__matricula') }} a
inner join (select distinct id_funcionario from {{ ref('raw_recursos_humanos_ergon_pgm__vinculo') }}) b on b.id_funcionario = a.id_funcionario