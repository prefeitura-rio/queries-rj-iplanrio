-- R-01 (matriz 04 §10): NOTA_FISCAL, CHAVE_ACESSO, DPS, DATA_COMPETENCIA e
-- DATA_COMPMUNICIPIO por NOTA_NACIONAL, entre DFEN.NOTAS_NACIONAIS ⨝ DPS (réplica) e
-- int_nc2__competencia. FULL OUTER JOIN pela chave, só NSU <= corte. Sem correção
-- catalogada.
with
    v2 as (
        select
            nota_nacional as chave,
            cast(nota_fiscal as numeric) as nota_fiscal,
            chave_acesso,
            dps,
            format_datetime('%Y-%m-%dT%H:%M:%S', data_competencia) as data_competencia,
            format_date('%Y-%m-%dT00:00:00', data_compmunicipio) as data_compmunicipio
        from {{ ref("int_nc2__competencia") }}
        where nsu <= {{ nc2_corte() }}
    ),

    oracle as (
        select
            n.nota_nacional as chave,
            n.nota_fiscal,
            n.chave_acesso,
            n.dps,
            d.data_competencia,
            n.data_compmunicipio
        from {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as n
        inner join
            {{ nc2_fonte("nota_carioca", "DPS") }} as d
            on d.dps = n.dps
            and d.data_competencia_municipio = n.data_compmunicipio
        where n.nsu <= {{ nc2_corte() }}
    ),

    comparado as (
        select
            coalesce(v.chave, o.chave) as chave,
            v.chave is not null as no_v2,
            o.chave is not null as no_oracle,
            (
                v.nota_fiscal is not distinct from o.nota_fiscal
                and v.chave_acesso is not distinct from o.chave_acesso
                and v.dps is not distinct from o.dps
                and v.data_competencia is not distinct from o.data_competencia
                and v.data_compmunicipio is not distinct from o.data_compmunicipio
            ) as colunas_iguais
        from v2 as v
        full outer join oracle as o on o.chave = v.chave
    )

select
    'R-01' as regra,
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
