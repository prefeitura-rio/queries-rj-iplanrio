{{
    config(
        alias='total_contagem',
        materialized="table",
        tags=["mart", "ergon", "contagem", "total", "tempo"],
        description="Tabela que armazena o resultado das contagens de tempo para benefícios dos servidores municipais."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.id_chave,
    t.finalidade,
    t.total_dias,
    t.diasfpub,
    t.diasfpubesp,
    t.total_periodos,
    t.total_anos,
    t.data_previsao_proximo_periodo,
    t.nome_finalidade_proximo_periodo,
    t.id_empresa
FROM {{ ref('raw_recursos_humanos_ergon__total_contagem') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and v.id_vinculo = t.id_vinculo


