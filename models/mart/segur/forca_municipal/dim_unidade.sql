{{
    config(
        alias="dim_unidade",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["tipo_unidade", "base_operacional"],
    )
}}

-- Dimensão de unidades da Força Municipal.
-- Fonte autoritativa: unidades_historico, que expõe unittype como campo nativo da API.
-- base_operacional é o único campo derivado por regex — centralizado aqui para
-- que nenhum modelo downstream precise derivá-lo.
with
    unidades_historico as (
        select * from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
    ),

    -- Uma linha por id_unidade.
    -- tipo_unidade e id_estacao vêm do campo nativo da API (sem regex).
    -- base_operacional vem da macro centralizada (único ponto de regex).
    -- id_agencia é estável por unidade — any_value é seguro.
    dim as (
        select
            id_unidade,
            any_value(tipo_unidade) as tipo_unidade,
            any_value(base_operacional) as base_operacional,
            any_value(id_estacao) as id_estacao,
            any_value(id_agencia) as id_agencia,
        from unidades_historico
        where id_unidade is not null
        group by id_unidade
    )

select *
from dim