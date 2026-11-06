{{
    config(
        partition_by={
            "field": "_bigquery_particao_data",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["cpf_cnpj_responsavel"],
    )
}}

-- R-11: notas exigíveis (detalhe). Mesmas 19 colunas, nomes, ordem e tipos de
-- mart_nota_carioca__notas_nacionais_exigiveis_detalhes (o teste nc2_colunas_iguais
-- confere): datas como texto AAAA-MM-DDTHH:MM:SS, números de DFEN.* como NUMERIC e
-- retencao como INT64, como saem do Oracle. Critério: STATUS_NOTA = 0, FISCALIZACAO
-- <> 2, CENARIO_EXIGIBILIDADE = 0 e responsável, prestador e contraparte existem em
-- pessoas (os INNER JOIN do Oracle). Todos os valores monetários passam sem
-- arredondamento.
with
    final as (
        select
            p.cpf_cnpj_responsavel as cpf_cnpj_responsavel,
            p.nome_contraparte as nome_contraparte,
            p.cpf_cnpj_contraparte as cpf_cnpj_contraparte,
            p.retencao as retencao,
            p.valor_servico as valor_servico,
            p.valor_ded_red as valor_ded_red,
            p.valor_issqn as valor_issqn,
            p.valor_base_calculo as valor_base_calculo,
            cast(p.nota_fiscal as numeric) as nota_fiscal,
            p.chave_acesso as chave_acesso,
            format_datetime('%Y-%m-%dT%H:%M:%S', p.data_validacao) as data_validacao,
            format_date(
                '%Y-%m-%dT00:00:00', p.data_compmunicipio
            ) as data_competencia_municipio,
            cast(p.tipo_retencao_issqn as numeric) as tipo_retencao,
            p.nota_nacional as nota_nacional,
            p.dps as dps,
            cast(p.opcao_simples_nacional as numeric) as opcao_simples_nacional,

            -- bigquery metadata
            p.data_compmunicipio as _bigquery_particao_data,

            current_datetime('America/Sao_Paulo') as _bigquery_updated_at
        from {{ ref("int_nc2__papeis") }} as p
        inner join {{ ref("int_nc2__status") }} as s on s.chave_acesso = p.chave_acesso
        inner join {{ ref("int_nc2__cenario") }} as c on c.chave_acesso = p.chave_acesso
        inner join
            {{ ref("int_nc2__fiscalizacao") }} as f on f.chave_acesso = p.chave_acesso
        where
            s.status_nota = 0
            and f.fiscalizacao <> 2
            and c.cenario_exigibilidade = 0
            and p.pessoas_resolvidas
    )

select
    *,
    {{ dbt_utils.generate_surrogate_key(["dps", "data_competencia_municipio"]) }}
    as _bigquery_uid
from final
