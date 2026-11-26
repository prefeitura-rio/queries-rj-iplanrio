with
    despesas_duplicadas as (
        select
            id_documento_osinfo,
            apontamento_duplicidade_indicador,
            apontamento_duplicidade_ids,
            declaracoes_contendo_nf_ids
        from {{ ref("mart_cgm_triagem_nf__despesas_classificada_cgm") }}
    ),

    ids_duplicidade_referenciados as (
        select
            d.id_documento_osinfo as id_origem,
            trim(id_alvo) as id_relacionado,
            'apontamento_duplicidade_ids' as coluna_origem
        from
            despesas_duplicadas d,
            unnest(split(d.apontamento_duplicidade_ids, ',')) as id_alvo
        where d.apontamento_duplicidade_indicador is true
    ),

    ids_rastreio_referenciados as (
        select
            d.id_documento_osinfo as id_origem,
            trim(id_alvo) as id_relacionado,
            'declaracoes_contendo_nf_ids' as coluna_origem
        from
            despesas_duplicadas d,
            unnest(split(d.declaracoes_contendo_nf_ids, ',')) as id_alvo
        where d.declaracoes_contendo_nf_ids is not null
    ),

    todos_ids_referenciados as (
        select *
        from ids_duplicidade_referenciados
        union all
        select *
        from ids_rastreio_referenciados
    )

-- 1. Integridade referencial dos IDs listados em duplicidade e rateio
select
    r.id_origem,
    r.id_relacionado,
    r.coluna_origem,
    'ID referenciado não encontrado no mart' as motivo_falha
from todos_ids_referenciados r
left join despesas_duplicadas alvo on r.id_relacionado = alvo.id_documento_osinfo
where alvo.id_documento_osinfo is null

union all

-- 2. Consistência entre apontamento_duplicidade_indicador,
-- apontamento_duplicidade_ids e declaracoes_contendo_nf_ids
select
    id_documento_osinfo as id_origem,
    coalesce(
        apontamento_duplicidade_ids, declaracoes_contendo_nf_ids
    ) as id_relacionado,
    'separacao_colunas' as coluna_origem,
    'Desalinhamento entre apontamento_duplicidade_indicador e colunas de IDs'
    as motivo_falha
from despesas_duplicadas
where
    (
        apontamento_duplicidade_indicador is true
        and (apontamento_duplicidade_ids is null or declaracoes_contendo_nf_ids is null)
    )
    or (
        apontamento_duplicidade_indicador is not true
        and apontamento_duplicidade_ids is not null
    )
