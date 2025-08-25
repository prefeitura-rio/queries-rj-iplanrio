-- - Consolidates CPF records from multiple Rio de Janeiro city systems (health,
-- social assistance, citizen services, transportation, and BCadastro) into a unified
-- view with source tracking and counting.
{{
    config(
        alias="all_cpfs",
        schema="intermediario_dados_mestres",
        materialized=('table' if target.name == 'dev' else 'ephemeral'),
    )
}}

with
    saude as (
        select distinct cpf, 'saude' as origem from {{ source("rj-sms", "paciente") }}
    ),

    cadunico as (
        select distinct cpf, 'cadunico' as origem
        from {{ source("rj-smas", "cadastros") }}
    ),

    chamados as (
        select distinct cpf, '1746' as origem
        from {{ source("rj-segovi", "1746_chamado_cpf") }}
    ),

    transporte as (
        select distinct cpf_cliente as cpf, 'transporte' as origem
        from {{ source("rj-smtr", "transacao_cpf") }}
        where cpf_particao is not null
    ),

    bcadastro as (
        select distinct b.cpf, 'bcadastro' as origem
        from {{ source("brutos_bcadastro", "cpf") }} as b
        where b.endereco.municipio = 'Rio de Janeiro'
    ),

    ergon as (
        select distinct lpad(id_cpf, 11, '0') as cpf, 'ergon' as origem
        from {{ source("rj-smfp", "funcionario") }}
    ),

    all_cpfs as (
        select *
        from saude
        union all
        select *
        from cadunico
        union all
        select *
        from chamados
        union all
        select *
        from transporte
        union all
        select *
        from bcadastro
        union all
        select *
        from ergon
    ),

    final_tb as (
        select
            cpf,
            array_agg(origem order by origem) as origens,
            count(*) as origens_count,
            cast(cpf as int64) as cpf_particao
        from all_cpfs
        -- # TODO: remover esse filtro e ver pq temos nulos no cpf
        WHERE cpf IS NOT NULL
        group by cpf
    )

select cpf, origens, origens_count, cpf_particao
from final_tb
