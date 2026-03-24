

{{
    config(

        alias='licenca_afastamento',
        materialized="table",
        tags=["mart", "ergon", "afastamento"],
        description="Tabela que armazena as ocorrências de afastamento do servidor municipal."
    )
}}

SELECT
    t.id_funcionario,
    t.id_vinculo,
    t.data_inicio,
    t.data_final,
    t.tipo_afastamento,
    t.id_afastamento,
    t.motivo,
    t.data_previsao_retorno,
    t.parecer,
    t.data_atendimento,
    t.id_empresa,
    t.crm
FROM {{ ref('raw_recursos_humanos_ergon__licenca_afastamento') }} AS t
inner join {{ ref('mart_recursos_humanos_ergon_pgm__vinculo') }} AS v on v.id_funcionario = t.id_funcionario and v.id_vinculo = t.id_vinculo



