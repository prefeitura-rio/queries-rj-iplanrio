{{
    config(
        alias='cargo',
        materialized="table",
        tags=["raw", "ergon", "cargo"],
        description="Tabela que contém os registros dos cargos para os quais os funcionários são nomeados em seus provimentos."
    )
}}

with cargos_provimentos as (
    select distinct id_cargo
    from {{ ref('raw_ergon_iplanrio__provimento') }}
)

SELECT 
    a.id_cargo,
    a.nome,
    a.categoria,
    a.subcategoria,
    a.tipo_controle_vaga,
    a.escolaridade,
    a.aglutinador,
    a.tipo_cargo,
    a.dt_extincao,
    a.cargo_funcao,
    a.updated_at
FROM {{ ref("raw_recursos_humanos_ergon__cargo") }} a
inner join cargos_provimentos b 
    on a.id_cargo = b.id_cargo