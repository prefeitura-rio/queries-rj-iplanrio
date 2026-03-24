{{
    config(
        alias='provimento',
        materialized="table",
        tags=["mart", "ergon", "provimento"],
        description="Eventos relacionados aos vínculos funcionais tanto com a administração direta como indireta da prefeitura do Rio de Janeiro."
    )
}}

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
    a.regime_horas,
    a.observacoes,
    a.id_empresa,
    a.updated_at as data_atualizacao
FROM {{ ref('raw_recursos_humanos_ergon__provimento')}} a -- pegando como origem o dbt original de tratamento dos setores da SMA
inner join (
    select distinct id_setor from {{ ref('mart_recursos_humanos_ergon_pgm__setor')}} 
    ) b on b.id_setor = a.id_setor -- e combinando apenas com os setores da PGM que foram tratados neste outro dbt, para garantir que traga apenas os provimentos relacionados à PGM



