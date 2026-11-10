{{
    config(
        partition_by={
            "field": "data_competencia_servico",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["chave_acesso"],
    )
}}

{#
    R-04: deduplicação do raw (append-only, com reprocessamentos) e corte IniExtDocsFiscais. Uma linha por nota nacional, já
    com as colunas da DPS que as regras seguintes usam.
      - NOTA_NACIONAL e DPS repetidos: sobrevive o menor NSU (NotasNacionais.vb:1750,1898); no mesmo NSU, a linha ingerida por
        último (reprocessamento do parser com versão nova).
      - Nota cuja competência do serviço (DPS.DATA_COMPETENCIA, não a municipal) é anterior a IniExtDocsFiscais é descartada
        (NotasNacionais.vb:1946), junto com a DPS.
      - Nota sem a DPS no raw não entra (o Oracle grava as duas juntas).
#}
with
    config as (select ini_ext_docs_fiscais from {{ ref("int_nc2__config") }}),

    nfse as (
        select
            nota_nacional,
            chave_acesso,
            nota_fiscal,
            dps,
            cod_local_incidencia,
            data_validacao,
            valor_ded_red,
            valor_base_calculo,
            aliquota,
            valor_issqn,
            chave_acesso_substituida,
            nsu
        from {{ nc2_raw("nfse") }}
        where nota_nacional is not null
        qualify
            row_number() over (
                partition by nota_nacional order by nsu asc, ingerido_em desc
            )
            = 1
    ),

    dps as (
        select
            dps,
            data_emissao,
            data_competencia,
            pessoa_prestador,
            pessoa_tomador,
            pessoa_intermediario,
            tributacao_nacional,
            tributacao_municipal,
            valor_servico,
            issqn,
            tipo_suspensao,
            tipo_retencao_issqn,
            nsu as nsu_dps
        from {{ nc2_raw("dps") }}
        where dps is not null
        qualify
            row_number() over (partition by dps order by nsu asc, ingerido_em desc) = 1
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
    d.nsu_dps,
    d.data_emissao,
    d.data_competencia,
    date(d.data_competencia) as data_competencia_servico,
    d.pessoa_prestador,
    d.pessoa_tomador,
    d.pessoa_intermediario,
    d.tributacao_nacional,
    d.tributacao_municipal,
    d.valor_servico,
    d.issqn,
    d.tipo_suspensao,
    d.tipo_retencao_issqn
from nfse as n
inner join dps as d on d.dps = n.dps
cross join config as c
where date(d.data_competencia) >= c.ini_ext_docs_fiscais
