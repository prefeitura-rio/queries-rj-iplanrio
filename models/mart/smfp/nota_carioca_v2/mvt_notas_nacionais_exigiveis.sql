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

-- R-11: resumo das exigíveis por (CPF/CNPJ do responsável, competência municipal,
-- tipo de retenção, opção pelo Simples, retenção). Mesmas 17 colunas, nomes, ordem e
-- tipos de mart_nota_carioca__notas_nacionais_exigiveis. SUM ignora NULL e devolve
-- NULL quando todas as notas do grupo são nulas, por isso os cnt_* acompanham cada
-- soma.
with
    notas as (select * from {{ ref("mvt_notas_nacionais_exigiveis_detalhes") }}),

    grouped as (
        select
            mv.cpf_cnpj_responsavel,
            mv.data_competencia_municipio,
            mv.tipo_retencao,
            mv.opcao_simples_nacional,
            mv.retencao,
            count(*) quantidade_notas,
            sum(mv.valor_servico) total_valor_servico,
            sum(mv.valor_ded_red) total_valor_ded_red,
            sum(mv.valor_issqn) total_valor_issqn,
            sum(mv.valor_base_calculo) total_valor_base_calculo,
            count(mv.valor_servico) cnt_valor_servico,
            count(mv.valor_ded_red) cnt_valor_ded_red,
            count(mv.valor_issqn) cnt_valor_issqn,
            count(mv.valor_base_calculo) cnt_valor_base_calculo
        from notas as mv
        group by
            mv.cpf_cnpj_responsavel,
            mv.data_competencia_municipio,
            mv.tipo_retencao,
            mv.opcao_simples_nacional,
            mv.retencao
    ),

    final as (
        select
            *,

            -- bigquery metadata
            safe_cast(
                safe.parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S', data_competencia_municipio
                ) as date
            ) as _bigquery_particao_data,

            current_datetime('America/Sao_Paulo') as _bigquery_updated_at,

            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "cpf_cnpj_responsavel",
                        "data_competencia_municipio",
                        "tipo_retencao",
                        "opcao_simples_nacional",
                        "retencao",
                    ]
                )
            }} as _bigquery_uid

        from grouped
    )

select *
from final
