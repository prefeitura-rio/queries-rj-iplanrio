{{
    config(
        alias="MVT_NOTAS_NACIONAIS_DETALHES",
        partition_by={
            "field": "_bigquery_particao_data",
            "data_type": "date",
            "granularity": "month",
        },
        materialized="table",
        job_execution_timeout_seconds=1800,
    )
}}


with
    notas_nacionais as (select * from {{ source("nota_carioca", "NOTAS_NACIONAIS") }}),

    dps as (select * from {{ source("nota_carioca", "DPS") }}),

    pessoas_nacionais as (
        select * from {{ source("nota_carioca", "PESSOAS_NACIONAIS") }}
    ),

    final as (

        select
            -- business logic
            n.nota_nacional,
            n.chave_acesso,
            n.nota_fiscal,
            n.dps,
            n.chave_acesso_substituida,
            n.chave_acesso_substituta,
            n.data_validacao,
            n.data_compmunicipio as data_competencia_municipio,
            n.status_nota,
            n.fiscalizacao,
            n.cenario_exigibilidade,
            n.cod_local_incidencia,
            n.aliquota,
            n.valor_base_calculo,
            n.valor_ded_red,
            n.valor_issqn,
            d.tipo_retencao_issqn,
            d.issqn,
            d.valor_servico,
            d.valor_desconto_incondicionado,
            d.grupo_servico,
            d.subgrupo_servico,
            d.servico_nacional,
            d.tributacao_nacional,
            d.tributacao_municipal,
            pe.cpf_cnpj as cpf_cnpj_emitente,
            pe.nome as nome_emitente,
            pp.cpf_cnpj as cpf_cnpj_prestador,
            pp.nome as nome_prestador,
            pp.opcao_simples_nacional,
            pp.regime_apuracao_simples,
            pp.regime_tributacao,
            pr.cpf_cnpj as cpf_cnpj_responsavel,
            pr.nome as nome_responsavel,
            pt.cpf_cnpj as cpf_cnpj_tomador,
            pt.nome as nome_tomador,
            pi.cpf_cnpj as cpf_cnpj_intermediario,
            pi.nome as nome_intermediario,
            n.nsu,
            n.data_cancelamento,
            n.pessoa_emitente,
            d.data_emissao,
            d.tipo_suspensao,
            d.beneficio,
            d.pessoa_prestador,
            d.pessoa_tomador,
            d.pessoa_intermediario,
            d.beneficio_nacional,
            d.valor_desconto_condicionado,
            -- n.rowid,
            -- d.rowid,
            -- pe.rowid,
            -- pp.rowid,
            -- pr.rowid,
            -- pt.rowid,
            -- pi.rowid,
            -- bigquery metadata
            safe_cast(
                safe.parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S', n.data_compmunicipio
                ) as date
            ) as _bigquery_particao_data,

            current_datetime('America/Sao_Paulo') as _bigquery_updated_at,

            {{ dbt_utils.generate_surrogate_key(["n.dps", "n.data_compmunicipio"]) }}
            as _bigquery_uid

        from notas_nacionais as n
        inner join
            dps as d
            on d.dps = n.dps
            and d.data_competencia_municipio = n.data_compmunicipio
        inner join pessoas_nacionais as pe on pe.pessoa_nacional = n.pessoa_emitente
        inner join pessoas_nacionais as pp on pp.pessoa_nacional = d.pessoa_prestador
        inner join pessoas_nacionais as pr on pr.pessoa_nacional = d.pessoa_responsavel
        inner join pessoas_nacionais as pt on pt.pessoa_nacional = d.pessoa_tomador
        inner join
            pessoas_nacionais as pi on pi.pessoa_nacional = d.pessoa_intermediario

    )

select *
from final
