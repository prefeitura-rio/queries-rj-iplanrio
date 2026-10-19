{{
    config(
        alias='provimento',
        materialized="table",
        tags=["raw", "ergon", "provimento"],
        description="Tabela que contém apenas os registros dos provimentos da IplanRio disponíveis no Ergon."
    )
}}

/*
    Filtrando apenas os registros dos provimentos da IplanRio disponíveis no Ergon, utilizando as tabela original de provimentos do Ergon e combinando com a tabela de setores da IplanRio.
*/
SELECT
    a.id_funcionario,
    a.id_vinculo,
    a.data_inicio,
    a.data_fim,
    a.id_setor,
    a.id_cargo,
    a.id_referencia,
    a.id_jornada,
    a.id_forma_provimento,
    a.observacoes,
    a.regime_horas,
    a.id_empresa,
    a.updated_at
FROM {{ ref("raw_recursos_humanos_ergon__provimento") }} a
inner join {{ ref("raw_ergon_iplanrio__setor") }} b 
    on a.id_setor = b.id_setor 
    and a.id_empresa = b.id_empresa
    and a.data_inicio between b.data_inicio and ifnull(b.data_fim, current_date("America/Sao_Paulo"))
