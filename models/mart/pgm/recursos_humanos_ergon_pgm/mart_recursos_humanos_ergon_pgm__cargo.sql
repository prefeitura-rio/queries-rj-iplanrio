{{
    config(
        alias='cargo',
        materialized="table",
        tags=["raw", "ergon", "cargo"],
        description="Tabela que contém os registros dos cargos para os quais os funcionários são nomeados em seus provimentos."
    )
}}

SELECT
    id_cargo,
    nome,
    categoria,
    subcategoria,
    tipo_controle_vaga,
    escolaridade,
    aglutinador,
    tipo_cargo,
    dt_extincao,
    cargo_funcao,
    updated_at as data_atualizacao
FROM {{ ref('raw_recursos_humanos_ergon__cargo') }} -- pegando como origem o dbt original de tratamento dos setores da SMA


