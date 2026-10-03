{{
    config(
        alias="MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES",
        partition_by={
            "field": "_bigquery_particao_data",
            "data_type": "date",
            "granularity": "month",
        },
        materialized="table",
        incremental_strategy="insert_overwrite",
        on_schema_change="fail",
    )
}}

-- variáveis
{% set current_day = modules.datetime.date.today().day %}
{% set lookback_months = 1 if current_day <= 10 else 0 %}


with
    notas_nacionais as (select * from {{ source("nota_carioca", "NOTAS_NACIONAIS") }}),

    dps as (select * from {{ source("nota_carioca", "DPS") }}),

    pessoas_nacionais as (
        select * from {{ source("nota_carioca", "PESSOAS_NACIONAIS") }}
    ),

    final as (

        select

            -- business logic
            pr.cpf_cnpj as cpf_cnpj_responsavel,
            pc.nome as nome_contraparte,
            pc.cpf_cnpj as cpf_cnpj_contraparte,
            case d.tipo_retencao_issqn when 1 then 0 else 1 end as retencao,
            d.valor_servico as valor_servico,
            n.valor_ded_red as valor_ded_red,
            n.valor_issqn as valor_issqn,
            n.valor_base_calculo as valor_base_calculo,
            n.nota_fiscal as nota_fiscal,
            n.chave_acesso as chave_acesso,
            n.data_validacao as data_validacao,
            n.data_compmunicipio as data_competencia_municipio,

            d.tipo_retencao_issqn as tipo_retencao,
            n.nota_nacional as nota_nacional,
            n.dps as dps,
            pp.opcao_simples_nacional as opcao_simples_nacional,

        -- n.rowid as nn_rowid,
        -- d.rowid as dps_rowid,
        -- pr.rowid as pr_rowid,
        -- pp.rowid as pp_rowid,
        -- pc.rowid as pc_rowid

        -- bigquery metadata
            safe_cast(
                safe.parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S', n.data_compmunicipio
                ) as date
            ) as _bigquery_particao_data,

            current_datetime('America/Sao_Paulo') as _bigquery_updated_at,

            {{
                dbt_utils.generate_surrogate_key(
                    ["n.dps", "n.data_compmunicipio"]
                )
            }} as _bigquery_uid,

        from
            notas_nacionais as n,
            dps as d,
            pessoas_nacionais as pr,
            pessoas_nacionais as pp,
            pessoas_nacionais as pc
        where
            n.status_nota = 0
            and n.fiscalizacao <> 2
            and n.cenario_exigibilidade = 0
            and d.dps = n.dps
            and d.data_competencia_municipio = n.data_compmunicipio
            and pr.pessoa_nacional = d.pessoa_responsavel
            and pp.pessoa_nacional = d.pessoa_prestador
            and pc.pessoa_nacional = d.pessoa_contraparte
    )

select *
from final
{% if is_incremental() %}
    where

        _bigquery_particao_data >= date_sub(
            date_trunc(current_date('America/Sao_Paulo'), month),
            interval {{ lookback_months }} month
        )

{% endif %}
