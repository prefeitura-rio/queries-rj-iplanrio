{{
    config(
        partition_by={
            "field": "data_compmunicipio",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["chave_acesso"],
    )
}}

{#
    R-06 e R-07: CENARIO_EXIGIBILIDADE (-1, 0..6) de cada nota, calculado do zero a cada execução. Como o estado atual do
    cadastro (MV_CONTRIBUINTES) é lido sempre, o cenário -1 nunca fica preso: some a janela de 7 dias da Fase 2 do PL/SQL (R-07).

    Cascata, com a correção catalogada B-04 (vale a ordem do VB para 4 e 5, e a habilitação só pesa no ISS próprio):
      4   competência do serviço (DPS) anterior a InicioPlanoB (Plano C)
      5   sem ISS: VALOR_ISSQN = 0, ISSQN <> 1 ou incidência diferente de 3304557 (código nulo conta como diferente, como no VB)
      -1  ISS próprio (tpRetISSQN 1) de responsável sem INSCRICAO_NOTANACIONAL = 1 em MV_CONTRIBUINTES (aguarda habilitação)
      1   ISS próprio de instituição financeira ativa em DESIFINSTITUICOES (STATUS = 0), pela raiz de 8 dígitos do CNPJ
      2   ISS próprio de concessionária de pedágio (MV_CONTRIBUINTES.PEDAGIO = 1) no grupo de serviço '22'
      3   ISS próprio de optante do Simples apurado pelo DAS (REGIME_APURACAO_SIMPLES = 1; nulo, 2 ou 3 não entra)
      6   exigibilidade suspensa (DPS.TIPO_SUSPENSAO 1 ou 2; NFSE-1003), de ISS próprio ou retido
      0   ISS devido: o resto, inclusive o ISS retido (tpRetISSQN 2 ou 3), que nunca precisa de habilitação
    Só o cenário 0 entra nas exigíveis (R-11).
#}
with
    notas as (select * from {{ ref("int_nc2__papeis") }}),

    config as (select inicio_plano_b from {{ ref("int_nc2__config") }}),

    habilitados as (
        select cpf_cnpj, logical_or(coalesce(pedagio, 0) = 1) as pedagio
        from {{ nc2_fonte("nota_carioca", "MV_CONTRIBUINTES") }}
        where inscricao_notanacional = 1 and cpf_cnpj is not null
        group by cpf_cnpj
    ),

    instituicoes_ativas as (
        select distinct raiz_cnpj
        from {{ nc2_fonte("nota_carioca", "DESIFINSTITUICOES") }}
        where status = 0 and raiz_cnpj is not null
    )

select
    n.nota_nacional,
    n.chave_acesso,
    n.dps,
    n.nsu,
    n.data_compmunicipio,
    n.tipo_retencao_issqn,
    h.cpf_cnpj is not null as responsavel_habilitado,
    i.raiz_cnpj is not null as instituicao_financeira_ativa,
    case
        when n.data_competencia_servico < c.inicio_plano_b
        then 4
        when
            coalesce(n.valor_issqn, 0) = 0
            or coalesce(n.issqn, 0) <> 1
            or coalesce(n.cod_local_incidencia, 'X') <> '3304557'
        then 5
        when n.tipo_retencao_issqn = 1 and h.cpf_cnpj is null
        then -1
        when n.tipo_retencao_issqn = 1 and i.raiz_cnpj is not null
        then 1
        when
            n.tipo_retencao_issqn = 1
            and coalesce(h.pedagio, false)
            and n.grupo_servico = '22'
        then 2
        when n.tipo_retencao_issqn = 1 and n.regime_apuracao_simples = 1
        then 3
        when n.tipo_suspensao in (1, 2)
        then 6
        else 0
    end as cenario_exigibilidade
from notas as n
cross join config as c
left join habilitados as h on h.cpf_cnpj = n.cpf_cnpj_responsavel
left join instituicoes_ativas as i on i.raiz_cnpj = substr(n.cpf_cnpj_responsavel, 1, 8)
