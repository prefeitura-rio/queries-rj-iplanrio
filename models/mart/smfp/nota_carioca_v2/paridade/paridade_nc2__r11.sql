-- R-11: as exigíveis do v2 contra as MVTs atuais do Oracle
-- (nota_carioca.MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES e
-- MVT_NOTAS_NACIONAIS_EXIGIVEIS), tolerância zero (valores monetários exatos, centavo
-- a centavo). Detalhe (chave 'D|<nota_nacional>'): as 16 colunas de negócio por
-- NOTA_NACIONAL. Resumo (chave 'R|<uid>'): contagens e somas por (CPF/CNPJ do
-- responsável, competência municipal, tipo de retenção, opção pelo Simples,
-- retenção). Corte: só NSU <= corte, e fora as notas que o v2 já alterou com evento
-- ou substituta de NSU acima do corte (o Oracle ainda não viu). No resumo, ficam fora
-- as dimensões em que entra alguma nota fora do corte, nos dois lados. Classes:
-- diferença que a paridade já reconheceu como correção catalogada da nota (R-06 B-04;
-- R-08 ou R-09 B-06) herda a classe; no resumo, a dimensão herda a correção se
-- nenhuma nota dela for divergente. O resto é divergente.
with
    afetadas as (
        select chave_acesso
        from {{ ref("int_nc2__status") }}
        where
            coalesce(nsu_max_evento, 0) > {{ nc2_corte() }}
            or coalesce(nsu_substituta, 0) > {{ nc2_corte() }}
    ),

    v2_detalhe as (
        select
            d.*,
            p.nsu,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "d.cpf_cnpj_responsavel",
                        "d.data_competencia_municipio",
                        "d.tipo_retencao",
                        "d.opcao_simples_nacional",
                        "d.retencao",
                    ]
                )
            }} as dim_uid,
            (p.nsu <= {{ nc2_corte() }} and a.chave_acesso is null) as comparavel
        from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }} as d
        inner join {{ ref("int_nc2__dedup") }} as p on p.nota_nacional = d.nota_nacional
        left join afetadas as a on a.chave_acesso = d.chave_acesso
    ),

    oracle_detalhe as (
        select
            m.*,
            n.nsu,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "m.cpf_cnpj_responsavel",
                        "m.data_competencia_municipio",
                        "m.tipo_retencao",
                        "m.opcao_simples_nacional",
                        "m.retencao",
                    ]
                )
            }} as dim_uid,
            (
                coalesce(n.nsu, 0) <= {{ nc2_corte() }} and a.chave_acesso is null
            ) as comparavel
        from
            {{
                nc2_fonte(
                    "nota_carioca_mvt", "MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES"
                )
            }} as m
        left join
            {{ nc2_fonte("nota_carioca", "NOTAS_NACIONAIS") }} as n
            on n.nota_nacional = m.nota_nacional
        left join afetadas as a on a.chave_acesso = m.chave_acesso
    ),

    comparado_detalhe as (
        select
            coalesce(v.nota_nacional, o.nota_nacional) as nota_nacional,
            coalesce(v.chave_acesso, o.chave_acesso) as chave_acesso,
            v.dim_uid as dim_uid_v2,
            o.dim_uid as dim_uid_oracle,
            v.nota_nacional is not null as no_v2,
            o.nota_nacional is not null as no_oracle,
            (
                v.cpf_cnpj_responsavel is not distinct from o.cpf_cnpj_responsavel
                and v.nome_contraparte is not distinct from o.nome_contraparte
                and v.cpf_cnpj_contraparte is not distinct from o.cpf_cnpj_contraparte
                and v.retencao is not distinct from o.retencao
                and v.valor_servico is not distinct from o.valor_servico
                and v.valor_ded_red is not distinct from o.valor_ded_red
                and v.valor_issqn is not distinct from o.valor_issqn
                and v.valor_base_calculo is not distinct from o.valor_base_calculo
                and v.nota_fiscal is not distinct from o.nota_fiscal
                and v.chave_acesso is not distinct from o.chave_acesso
                and v.data_validacao is not distinct from o.data_validacao
                and v.data_competencia_municipio
                is not distinct from o.data_competencia_municipio
                and v.tipo_retencao is not distinct from o.tipo_retencao
                and v.dps is not distinct from o.dps
                and v.opcao_simples_nacional
                is not distinct from o.opcao_simples_nacional
            ) as colunas_iguais
        from (select * from v2_detalhe where comparavel) as v
        full outer join
            (select * from oracle_detalhe where comparavel) as o
            on o.nota_nacional = v.nota_nacional
    ),

    classe_detalhe as (
        select
            c.*,
            case
                when c.no_v2 and c.no_oracle and c.colunas_iguais
                then 'igual'
                when r06.classe = 'correcao_catalogada:B-04'
                then 'correcao_catalogada:B-04'
                when coalesce(r08.classe, r09.classe) = 'correcao_catalogada:B-06'
                then 'correcao_catalogada:B-06'
                else 'divergente'
            end as classe,
            case
                when not c.no_oracle
                then 'so_v2'
                when not c.no_v2
                then 'so_oracle'
                when not c.colunas_iguais
                then 'colunas_diferentes'
            end as detalhe
        from comparado_detalhe as c
        left join {{ ref("paridade_nc2__r06") }} as r06 on r06.chave = c.nota_nacional
        left join {{ ref("paridade_nc2__r08") }} as r08 on r08.chave = c.chave_acesso
        left join {{ ref("paridade_nc2__r09") }} as r09 on r09.chave = c.chave_acesso
    ),

    dimensoes_fora_do_corte as (
        select dim_uid
        from v2_detalhe
        where not comparavel
        union distinct
        select dim_uid
        from oracle_detalhe
        where not comparavel
    ),

    classes_por_dimensao as (
        select
            dim_uid,
            logical_or(classe = 'divergente') as tem_divergente,
            min(
                if(starts_with(classe, 'correcao_catalogada:'), classe, null)
            ) as correcao
        from
            (
                select dim_uid_v2 as dim_uid, classe
                from classe_detalhe
                where dim_uid_v2 is not null
                union all
                select dim_uid_oracle as dim_uid, classe
                from classe_detalhe
                where dim_uid_oracle is not null
            )
        group by dim_uid
    ),

    v2_resumo as (
        select
            r.cpf_cnpj_responsavel,
            r.data_competencia_municipio,
            r.tipo_retencao,
            r.opcao_simples_nacional,
            r.retencao,
            r.quantidade_notas,
            r.total_valor_servico,
            r.total_valor_ded_red,
            r.total_valor_issqn,
            r.total_valor_base_calculo,
            r.cnt_valor_servico,
            r.cnt_valor_ded_red,
            r.cnt_valor_issqn,
            r.cnt_valor_base_calculo,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "r.cpf_cnpj_responsavel",
                        "r.data_competencia_municipio",
                        "r.tipo_retencao",
                        "r.opcao_simples_nacional",
                        "r.retencao",
                    ]
                )
            }} as dim_uid
        from {{ ref("mvt_notas_nacionais_exigiveis") }} as r
    ),

    oracle_resumo as (
        select
            r.cpf_cnpj_responsavel,
            r.data_competencia_municipio,
            r.tipo_retencao,
            r.opcao_simples_nacional,
            r.retencao,
            r.quantidade_notas,
            r.total_valor_servico,
            r.total_valor_ded_red,
            r.total_valor_issqn,
            r.total_valor_base_calculo,
            r.cnt_valor_servico,
            r.cnt_valor_ded_red,
            r.cnt_valor_issqn,
            r.cnt_valor_base_calculo,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "r.cpf_cnpj_responsavel",
                        "r.data_competencia_municipio",
                        "r.tipo_retencao",
                        "r.opcao_simples_nacional",
                        "r.retencao",
                    ]
                )
            }} as dim_uid
        from {{ nc2_fonte("nota_carioca_mvt", "MVT_NOTAS_NACIONAIS_EXIGIVEIS") }} as r
    ),

    comparado_resumo as (
        select
            coalesce(v.dim_uid, o.dim_uid) as dim_uid,
            v.dim_uid is not null as no_v2,
            o.dim_uid is not null as no_oracle,
            (
                v.quantidade_notas is not distinct from o.quantidade_notas
                and v.total_valor_servico is not distinct from o.total_valor_servico
                and v.total_valor_ded_red is not distinct from o.total_valor_ded_red
                and v.total_valor_issqn is not distinct from o.total_valor_issqn
                and v.total_valor_base_calculo
                is not distinct from o.total_valor_base_calculo
                and v.cnt_valor_servico is not distinct from o.cnt_valor_servico
                and v.cnt_valor_ded_red is not distinct from o.cnt_valor_ded_red
                and v.cnt_valor_issqn is not distinct from o.cnt_valor_issqn
                and v.cnt_valor_base_calculo
                is not distinct from o.cnt_valor_base_calculo
            ) as colunas_iguais
        from
            (
                select *
                from v2_resumo as x
                where
                    not exists (
                        select 1
                        from dimensoes_fora_do_corte as f
                        where f.dim_uid = x.dim_uid
                    )
            ) as v
        full outer join
            (
                select *
                from oracle_resumo as y
                where
                    not exists (
                        select 1
                        from dimensoes_fora_do_corte as f
                        where f.dim_uid = y.dim_uid
                    )
            ) as o
            on o.dim_uid = v.dim_uid
    )

select 'R-11' as regra, concat('D|', nota_nacional) as chave, classe, detalhe
from classe_detalhe
union all
select
    'R-11' as regra,
    concat('R|', c.dim_uid) as chave,
    case
        when c.no_v2 and c.no_oracle and c.colunas_iguais
        then 'igual'
        when d.correcao is not null and not d.tem_divergente
        then d.correcao
        else 'divergente'
    end as classe,
    case
        when not c.no_oracle
        then 'so_v2'
        when not c.no_v2
        then 'so_oracle'
        when not c.colunas_iguais
        then 'colunas_diferentes'
    end as detalhe
from comparado_resumo as c
left join classes_por_dimensao as d on d.dim_uid = c.dim_uid
