{{
    config(
        alias="funcionarios",
        schema="recursos_humanos_ergon_saude",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}



with
    funcionarios as (
        select
            f.id_vinculo as id_funcionario,
            lpad(f.id_cpf, 11, '0') as cpf,  -- Adiciona zero à esquerda caso o CPF tenha menos de 11 dígitos
            f.nome
        from {{ ref("raw_recursos_humanos_ergon__funcionario") }} f
        where f.id_cpf is not null  -- and lpad(f.id_cpf, 11, '0') in ('')
    ),

    provimento as (
        select
            p.id_funcionario,
            p.id_vinculo,
            p.data_inicio as provimento_inicio,
            p.data_fim as provimento_fim,
            p.id_setor,
            p.id_cargo,
            p.empresa_vinculo as id_empresa
        from {{ ref("raw_recursos_humanos_ergon__provimento") }} p
        -- get the most recent id_vinculo
        qualify
            row_number() over (
                partition by id_funcionario order by id_vinculo desc, data_inicio desc
            )
            = 1
    ),

    setor as (
        select
            id_setor,
            data_inicio as setor_inicio,
            data_fim as setor_fim,
            id_setor_pai,
            nome as setor_nome,
            sigla as setor_sigla,
            id_secretaria
        from  {{ ref("raw_recursos_humanos_ergon__setor") }}
        -- get the most recent of the setor
        qualify row_number() over (partition by id_setor order by data_inicio desc) = 1
    ),

    cargo as (
        select
            id_cargo,
            nome as cargo_nome,
            categoria as cargo_categoria,
            subcategoria as cargo_subcategoria,
        from  {{ ref("raw_recursos_humanos_ergon__cargo") }}
    ),

    vacancia_vinculo as (
        select id_funcionario, id_vinculo, data_vacancia, id_matricula_vinculo as matricula
        from  {{ ref("raw_recursos_humanos_ergon__vinculo") }}
    -- get the most recent id_vinculo
    -- qualify
    -- row_number() over (partition by id_funcionario order by id_vinculo desc) = 1
    ),

    empresa as (
        select
            id_empresa,
            nome_empresa as empresa_nome,
            sigla as empresa_sigla,
            cnpj as empresa_cnpj
        from  {{ ref("raw_recursos_humanos_ergon__empresas") }}
    ),

    secretaria as (
        select
            safe_cast(regexp_replace(cd_ua, r'\.0$', '') as string) as id_secretaria,
            safe_cast(
                regexp_replace(sigla_ua, r'\.0$', '') as string
            ) as secretaria_sigla,
            safe_cast(regexp_replace(nome_ua, r'\.0$', '') as string) as secretaria_nome
        from {{ source('unidades_administrativas_staging', 'orgaos')}}
    ),

    funcionarios_saude as (
        select
            f.cpf,
            array_agg(
                struct(
                    f.id_funcionario,
                    vv.matricula,
                    f.nome,
                    case
                        when (p.provimento_fim is null) and (vv.data_vacancia is null)
                        then true
                        else false
                    end as status_ativo,
                    p.provimento_inicio,
                    p.provimento_fim,
                    vv.data_vacancia,
                    s.id_secretaria,
                    sec.secretaria_sigla,
                    sec.secretaria_nome,
                    p.id_empresa,
                    s.setor_nome,
                    s.setor_sigla,
                    s.setor_inicio,
                    s.setor_fim,
                    c.cargo_nome,
                    c.cargo_categoria,
                    c.cargo_subcategoria,
                    emp.empresa_nome,
                    emp.empresa_sigla,
                    emp.empresa_cnpj
                )
            ) as dados,
            safe_cast(f.cpf as int64) as cpf_particao
        from funcionarios f
        left join provimento p on f.id_funcionario = p.id_funcionario
        left join setor s on p.id_setor = s.id_setor
        left join cargo c on p.id_cargo = c.id_cargo
        left join
            vacancia_vinculo vv
            on f.id_funcionario = vv.id_funcionario
            and p.id_vinculo = vv.id_vinculo
        left join empresa emp on p.id_empresa = emp.id_empresa
        left join secretaria sec on s.id_secretaria = sec.id_secretaria

        where
            s.id_secretaria in ('1800', '1851')
            or p.id_empresa in (
                '32',
                '80',
                '81',
                '82',
                '83',
                '84',
                '85',
                '86',
                '87',
                '88',
                '89',
                '97',
                '23'
            )

        group by f.cpf
    )

select
    *,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP(), "America/Sao_Paulo") as updated_at
from funcionarios_saude

