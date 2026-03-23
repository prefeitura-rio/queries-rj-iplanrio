
{{
    config(
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        alias='vantagens',
        materialized="table",
        tags=["raw", "ergon", "vantagens"],
        description="Tabela que armazena informações sobre atributos cadastrados que representam vantagens (valores a receber) dos servidores."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.nome_vantagem,
    t.data_inicio,
    t.data_final,
    t.valor_vantagem,
    t.informacao_atributo,
    t.tipo_incorporacao_cargo_fiducia,
    t.percentual_incorporacao_cargo_fiducia,
    t.incide_tabela_vencimentos,
    t.incide_tabela_simbolo,
    t.observacoes,
    t.valor_complementar_1,
    t.informacao_complementar_1,
    t.valor_complementar_2,
    t.informacao_complementar_2,
    t.valor_complementar_3,
    t.informacao_complementar_3,
    t.valor_complementar_4,
    t.informacao_complementar_4,
    t.valor_complementar_5,
    t.informacao_complementar_5,
    t.valor_incorporado,
    t.id_empresa,
    t.id_vantagem,
    t.data_particao
FROM {{ ref('raw_recursos_humanos_ergon__vantagens') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and v.id_vinculo = t.id_vinculo



