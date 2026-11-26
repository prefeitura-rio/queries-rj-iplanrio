{{
    config(
        schema="poc_cgm_triagem_nf",
        alias="despesas_classificada_cgm",
        materialized="table",
    )
}}

with
    despesas_base as (
        select * from {{ ref("mart_cgm_triagem_nf__despesas_classificada") }}
    ),

    -- Recálculo do excedente: Grave apenas se diferença > R$ 10.000; demais
    -- excedentes seguem como Leve
    indicadores_recalculados as (
        select
            * except (
                apontamento_valor_pago_excedente_indicador,
                apontamento_emissao_anterior_abertura_indicador
            ),
            apontamento_valor_pago_excedente_indicador
            as apontamento_valor_pago_excedente_original,
            case
                when
                    apontamento_valor_pago_excedente_indicador is true
                    and (
                        cast(declaracao_pago_valor as float64)
                        - extracao_documento_valor
                    )
                    > 10000.0
                then true
                when apontamento_valor_pago_excedente_indicador is null
                then null
                else false
            end as apontamento_valor_pago_excedente_indicador
        from despesas_base
    ),

    -- =========================================================================
    -- RECÁLCULO DOS APONTAMENTOS E SEPARAÇÃO DO RASTREIO DE DUPLICIDADE
    -- =========================================================================
    despesas_reclassificadas as (
        select
            -- Chave primária e contexto administrativo
            id_documento as id_documento_osinfo,
            contrato_numero as contrato_numero_osinfo,
            contrato_periodo_vigencia,
            secretaria_nome as secretaria_nome_osinfo,
            id_organizacao,
            organizacao_nome as organizacao_nome_osinfo,
            id_unidade,
            unidade_nome,

            -- Dados da declaração (OSINFO)
            declaracao_envio_data,
            declaracao_arquivo_nome,
            declaracao_referencia_ano as declaracao_referencia_ano_osinfo,
            declaracao_referencia_mes as declaracao_referencia_mes_osinfo,
            declaracao_numero_documento,
            declaracao_cnpj_cpf,
            declaracao_documento_valor as declaracao_documento_valor_osinfo,
            declaracao_pago_valor as declaracao_pago_valor_osinfo,
            declaracao_emissao_data as declaracao_emissao_data_osinfo,
            declaracao_pagamento_data,

            -- Dados da extração (Agente de IA)
            extracao_resultado as extracao_resultado_agente_ia,
            extracao_pagina,
            extracao_documento_tipo,
            extracao_numero_documento,
            extracao_cnpj_cpf,
            extracao_documento_valor as extracao_documento_valor_agente_ia,
            extracao_emissao_data as extracao_emissao_data_agente_ia,
            extracao_paginas_com_match,
            extracao_paginas_sem_match,

            -- Dados do fornecedor (bCadastro)
            fornecedor_razao_social,
            fornecedor_uf,
            fornecedor_cnae_descricao,
            fornecedor_situacao_cadastral_atual,

            -- Classificação final recalculada: excedente > 10k fica Grave; excedente
            -- <= 10k vai para Leve
            case
                when extracao_resultado = 'NF Sem Match'
                then 'Não avaliado'
                when extracao_resultado = 'NF Não Encontrada'
                then 'Grave'
                when extracao_resultado = 'NF Encontrada'
                then
                    case
                        when
                            apontamento_duplicidade_indicador is true
                            or apontamento_cancelamento_indicador is true
                            or apontamento_cnpj_inativo_indicador is true
                            or apontamento_valor_pago_excedente_indicador is true
                        then 'Grave'
                        when
                            apontamento_emissao_divergente_indicador is true
                            or apontamento_valor_divergente_indicador is true
                            or apontamento_valor_pago_excedente_original is true
                        then 'Leve'
                        else 'Descartado'
                    end
                else null
            end as apontamento_resultado,

            -- Indicadores mantidos e separação de duplicidade vs. rastreio completo
            -- na mesma NF
            apontamento_duplicidade_indicador,
            case
                when apontamento_duplicidade_indicador is true
                then apontamento_duplicidade_ids
                else null
            end as apontamento_duplicidade_ids,
            apontamento_duplicidade_ids as declaracoes_contendo_nf_ids,
            apontamento_cancelamento_indicador,
            apontamento_cnpj_inativo_indicador,
            apontamento_valor_pago_excedente_indicador,
            apontamento_emissao_divergente_indicador,
            apontamento_valor_divergente_indicador,

            -- Metadados de processamento
            versao_pipeline,
            data_processamento,
            timestamp_processamento,
            current_timestamp() as timestamp_geracao
        from indicadores_recalculados
    )

select *
from despesas_reclassificadas
