with despesas_duplicadas as (
    select
        id_documento,
        apontamento_duplicidade_indicador,
        apontamento_duplicidade_ids
    from {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
),

ids_referenciados as (
    select
        d.id_documento as id_origem,
        trim(id_alvo) as id_relacionado
    from despesas_duplicadas d,
    unnest(split(d.apontamento_duplicidade_ids, ',')) as id_alvo
    where d.apontamento_duplicidade_indicador is true
)

select
    r.id_origem,
    r.id_relacionado,
    alvo.apontamento_duplicidade_indicador as status_alvo
from ids_referenciados r
left join despesas_duplicadas alvo
    on r.id_relacionado = alvo.id_documento
where alvo.id_documento is null
   or alvo.apontamento_duplicidade_indicador is not true