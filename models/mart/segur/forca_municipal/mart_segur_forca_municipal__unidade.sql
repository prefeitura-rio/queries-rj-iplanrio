{{
    config(
        alias="unidade",
        schema="forca_municipal",
        materialized="table",
        cluster_by=["tipo_unidade", "base_operacional"],
    )
}}

-- Dimensão de unidades da Força Municipal.
-- Fonte autoritativa: unidades_historico, que expõe unittype como campo nativo da API.
-- base_operacional é o único campo derivado por regex — centralizado aqui para
-- que nenhum modelo downstream precise derivá-lo.
-- Uma linha por id_unidade via QUALIFY: estado mais recente do registro.
with
    unidades_historico as (
        select * from {{ ref("raw_segur_forca_municipal__unidades_historico") }}
    )

select id_unidade, tipo_unidade, base_operacional, id_estacao, id_agencia
from unidades_historico
where id_unidade is not null
qualify
    row_number() over (
        partition by id_unidade order by data_hora_criacao desc, ordem_criacao desc
    )
    = 1