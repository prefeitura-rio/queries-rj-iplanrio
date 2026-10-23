-- R-02: responsável e contraparte por tpRetISSQN, sentinela zero-GUID para tomador
-- ausente, colunas de serviço e dedução.
with
    papeis as (select * from {{ ref("int_nc2__papeis") }}),

    esperado as (
        select
            'A01_NAO_RETIDA' as chave_acesso,
            '98000001000101' as cpf_cnpj_responsavel,
            '98100001000101' as cpf_cnpj_contraparte,
            0 as retencao,
            '01' as grupo_servico,
            '0101' as subgrupo_servico,
            '010101001' as servico_nacional,
            cast(1 as int64) as opcao_simples_nacional
        union all
        select
            'A03_RETIDA_TOMADOR',
            '98100001000101',
            '98000001000101',
            1,
            '01',
            '0101',
            '010101001',
            1
        union all
        select
            'A04_RETIDA_INTERMEDIARIO',
            '98200001000101',
            '98000001000101',
            1,
            '01',
            '0101',
            '010101001',
            1
        union all
        select
            'A05_SEM_TOMADOR',
            '98000001000101',
            '00000000000000',
            0,
            '01',
            '0101',
            '010101001',
            1
        union all
        select
            'A06_SERVICO_22',
            '98000008000108',
            '98100003001',
            0,
            '22',
            '2201',
            '220101012',
            1
        union all
        select
            'H01_SIMPLES_REGIME_2',
            '98000003000103',
            '98100001000101',
            0,
            '01',
            '0101',
            '010101001',
            3
    ),

    falhas as (
        select
            e.chave_acesso,
            'colunas de papéis ou de serviço diferentes do esperado' as falha
        from esperado as e
        left join papeis as p on p.chave_acesso = e.chave_acesso
        where
            p.cpf_cnpj_responsavel is distinct from e.cpf_cnpj_responsavel
            or p.cpf_cnpj_contraparte is distinct from e.cpf_cnpj_contraparte
            or p.retencao is distinct from e.retencao
            or p.grupo_servico is distinct from e.grupo_servico
            or p.subgrupo_servico is distinct from e.subgrupo_servico
            or p.servico_nacional is distinct from e.servico_nacional
            or p.opcao_simples_nacional is distinct from e.opcao_simples_nacional
        union all
        select
            chave_acesso,
            'sem tomador: contraparte deve ser a sentinela zero-GUID, CPF/CNPJ de 14 zeros e nome nulo'
        from papeis
        where
            chave_acesso = 'A05_SEM_TOMADOR'
            and (
                pessoa_contraparte is distinct from '00000000000000000000000000000000'
                or cpf_cnpj_contraparte is distinct from '00000000000000'
                or nome_contraparte is not null
            )
        union all
        select
            chave_acesso,
            'tpRetISSQN 2 e 3 deixam o prestador como contraparte e tipo_iss 1'
        from papeis
        where
            chave_acesso in ('A03_RETIDA_TOMADOR', 'A04_RETIDA_INTERMEDIARIO')
            and (
                pessoa_contraparte is distinct from pessoa_prestador
                or tipo_iss is distinct from 1
            )
        union all
        select chave_acesso, 'ISS próprio tem tipo_iss 0'
        from papeis
        where chave_acesso = 'A01_NAO_RETIDA' and tipo_iss is distinct from 0
        union all
        select chave_acesso, 'VALOR_DED_RED passa sem alteração (10,50 e nulo)'
        from papeis
        where
            (chave_acesso = 'A07_DEDUCAO' and valor_ded_red is distinct from 10.50)
            or (chave_acesso = 'A08_DEDUCAO_NULA' and valor_ded_red is not null)
        union all
        select
            chave_acesso,
            'responsável, prestador e contraparte: só a nota com tomador fantasma fica sem pessoa'
        from papeis
        where
            (chave_acesso = 'J01_PESSOA_AUSENTE' and pessoas_resolvidas)
            or (chave_acesso = 'A01_NAO_RETIDA' and not pessoas_resolvidas)
            or (chave_acesso = 'A05_SEM_TOMADOR' and not pessoas_resolvidas)
    )

select *
from falhas
