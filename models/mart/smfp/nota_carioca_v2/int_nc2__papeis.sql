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

-- R-02: papéis da nota e colunas derivadas da DPS (Dps.vb:65-67,1468-1476;
-- NotasNacionais.vb:684-693). tpRetISSQN 1: responsável = prestador, contraparte =
-- tomador (ou a sentinela zero-GUID se não há tomador); tpRetISSQN 2: responsável =
-- tomador, contraparte = prestador; tpRetISSQN 3: responsável = intermediário. A
-- sentinela (ADR-041) aponta para a linha de PESSOAS_NACIONAIS com CPF_CNPJ
-- '00000000000000' e NOME nulo. OPCAO_SIMPLES_NACIONAL e REGIME_APURACAO_SIMPLES vêm
-- sempre do prestador (ADR-092). As pessoas vêm do raw por HASH_ID (o raw não tem o
-- GUID do Oracle). pessoas_resolvidas diz se responsável, prestador e contraparte
-- existem em pessoas: o Oracle os junta por INNER JOIN, e a R-11 repete esse corte.
with
    notas as (select * from {{ ref("int_nc2__competencia") }}),

    pessoas as (select * from {{ ref("int_nc2__dedup_pessoas") }}),

    com_hash as (
        select
            *,
            case
                tipo_retencao_issqn
                when 2
                then pessoa_tomador
                when 3
                then pessoa_intermediario
                else pessoa_prestador
            end as pessoa_responsavel,
            case
                when tipo_retencao_issqn in (2, 3)
                then pessoa_prestador
                else coalesce(pessoa_tomador, '00000000000000000000000000000000')
            end as pessoa_contraparte
        from notas
    )

select
    n.nota_nacional,
    n.chave_acesso,
    n.nota_fiscal,
    n.dps,
    n.cod_local_incidencia,
    n.data_validacao,
    n.valor_ded_red,
    n.valor_base_calculo,
    n.aliquota,
    n.valor_issqn,
    n.chave_acesso_substituida,
    n.nsu,
    n.nsu_dps,
    n.data_emissao,
    n.data_competencia,
    n.data_competencia_servico,
    n.data_compmunicipio,
    n.tipo_retencao_issqn,
    n.issqn,
    n.tipo_suspensao,
    n.tributacao_nacional,
    n.tributacao_municipal,
    n.valor_servico,
    n.pessoa_prestador,
    n.pessoa_responsavel,
    n.pessoa_contraparte,
    pr.cpf_cnpj as cpf_cnpj_responsavel,
    pp.cpf_cnpj as cpf_cnpj_prestador,
    if(
        n.pessoa_contraparte = '00000000000000000000000000000000',
        '00000000000000',
        pc.cpf_cnpj
    ) as cpf_cnpj_contraparte,
    if(
        n.pessoa_contraparte = '00000000000000000000000000000000', null, pc.nome
    ) as nome_contraparte,
    pp.opcao_simples_nacional,
    pp.regime_apuracao_simples,
    case n.tipo_retencao_issqn when 1 then 0 else 1 end as retencao,
    case
        when n.tipo_retencao_issqn = 1
        then 0
        when n.tipo_retencao_issqn in (2, 3)
        then 1
    end as tipo_iss,
    substr(lpad(cast(n.tributacao_nacional as string), 6, '0'), 1, 2) as grupo_servico,
    substr(
        lpad(cast(n.tributacao_nacional as string), 6, '0'), 1, 4
    ) as subgrupo_servico,
    concat(
        lpad(cast(n.tributacao_nacional as string), 6, '0'),
        coalesce(lpad(cast(n.tributacao_municipal as string), 3, '0'), '')
    ) as servico_nacional,
    (
        pr.hash_id is not null
        and pp.hash_id is not null
        and (
            n.pessoa_contraparte = '00000000000000000000000000000000'
            or pc.hash_id is not null
        )
    ) as pessoas_resolvidas
from com_hash as n
left join pessoas as pr on pr.hash_id = n.pessoa_responsavel
left join pessoas as pp on pp.hash_id = n.pessoa_prestador
left join pessoas as pc on pc.hash_id = n.pessoa_contraparte
