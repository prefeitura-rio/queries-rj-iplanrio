{{ config(materialized="table") }}

-- Corte de NSU da paridade F1: uma linha. Vale var('nc2_corte_nsu') quando informada
-- (o flow de paridade pode passar o MAX(NSU) de MV_DOCUMENTOSFISCAISNACIONAIS com
-- Carga_Base_Pcrj processado); sem ela, o maior NSU das réplicas do Oracle
-- (NOTAS_NACIONAIS e EVENTOS_DFEN), o maior NSU que o Oracle já extraiu. Todo modelo
-- paridade_nc2__* só compara NSU <= corte_nsu.
with
    oracle as (
        select
            (
                select cast(max(nsu) as int64)
                from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }}
            ) as max_nsu_notas,
            (
                select cast(max(nsu) as int64)
                from {{ nc2_fonte("nota_carioca", "EVENTOS_DFEN") }}
            ) as max_nsu_eventos
    )

select
    coalesce(
        {% if var("nc2_corte_nsu", none) is not none -%}
            cast({{ var("nc2_corte_nsu") | int }} as int64)
        {%- else -%}cast(null as int64)
        {%- endif %},
        greatest(coalesce(max_nsu_notas, 0), coalesce(max_nsu_eventos, 0))
    ) as corte_nsu
from oracle
