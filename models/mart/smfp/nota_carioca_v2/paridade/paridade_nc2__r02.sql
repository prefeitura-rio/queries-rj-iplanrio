-- R-02: CPF_CNPJ_RESPONSAVEL, CPF_CNPJ_CONTRAPARTE (inclui a sentinela
-- '00000000000000'), GRUPO_SERVICO, SUBGRUPO_SERVICO, SERVICO_NACIONAL, RETENCAO,
-- OPCAO_SIMPLES_NACIONAL e VALOR_DED_RED por NOTA_NACIONAL, entre o Oracle
-- (NOTAS_NACIONAIS ⨝ DPS ⨝ PESSOAS_NACIONAIS) e int_nc2__papeis. Valores monetários
-- sem tolerância. Só NSU <= corte.
with
    v2 as (
        select
            nota_nacional as chave,
            cpf_cnpj_responsavel,
            cpf_cnpj_contraparte,
            grupo_servico,
            subgrupo_servico,
            servico_nacional,
            retencao,
            cast(opcao_simples_nacional as numeric) as opcao_simples_nacional,
            valor_ded_red
        from {{ ref("int_nc2__papeis") }}
        where nsu <= {{ nc2_corte() }}
    ),

    oracle as (
        select
            n.nota_nacional as chave,
            pr.cpf_cnpj as cpf_cnpj_responsavel,
            pc.cpf_cnpj as cpf_cnpj_contraparte,
            d.grupo_servico,
            d.subgrupo_servico,
            d.servico_nacional,
            case d.tipo_retencao_issqn when 1 then 0 else 1 end as retencao,
            pp.opcao_simples_nacional,
            n.valor_ded_red
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as n
        inner join
            {{ nc2_fonte("nota_carioca", "DPS") }} as d
            on d.dps = n.dps
            and d.data_competencia_municipio = n.data_compmunicipio
        left join
            {{ nc2_fonte("nota_carioca", "PESSOAS_NACIONAIS") }} as pr
            on pr.pessoa_nacional = d.pessoa_responsavel
        left join
            {{ nc2_fonte("nota_carioca", "PESSOAS_NACIONAIS") }} as pc
            on pc.pessoa_nacional = d.pessoa_contraparte
        left join
            {{ nc2_fonte("nota_carioca", "PESSOAS_NACIONAIS") }} as pp
            on pp.pessoa_nacional = d.pessoa_prestador
        where n.nsu <= {{ nc2_corte() }}
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            (
                v.cpf_cnpj_responsavel is not distinct from o.cpf_cnpj_responsavel
                and v.cpf_cnpj_contraparte is not distinct from o.cpf_cnpj_contraparte
                and v.grupo_servico is not distinct from o.grupo_servico
                and v.subgrupo_servico is not distinct from o.subgrupo_servico
                and v.servico_nacional is not distinct from o.servico_nacional
                and v.retencao is not distinct from o.retencao
                and v.opcao_simples_nacional
                is not distinct from o.opcao_simples_nacional
                and v.valor_ded_red is not distinct from o.valor_ded_red
            ) as colunas_iguais
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-02' as regra,
    chave,
    if(no_v2 and no_oracle and colunas_iguais, 'igual', 'divergente') as classe,
    case
        when not no_oracle
        then 'so_v2'
        when not no_v2
        then 'so_oracle'
        when not colunas_iguais
        then 'colunas_diferentes'
    end as detalhe
from comparado
