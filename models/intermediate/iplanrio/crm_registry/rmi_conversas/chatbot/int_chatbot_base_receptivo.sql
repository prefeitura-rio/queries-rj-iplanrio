{{
    config(
        materialized="incremental",
        schema="intermediario_rmi_conversas",
        tags=["quarter_hourly"],
        unique_key=["id_sessao", "inicio_datahora"],
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}

with
    source as (
        select * from {{ source("brutos_wetalkie_staging", "fluxos_ura") }}
        {% if is_incremental() %}
            -- Filtra dados para processamento incremental baseado na data de início do atendimento
            where
                date(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez', json_value(json_data, '$.beginDate')
                ),
                'America/Sao_Paulo'
                )
                >= (select max(data_particao) from {{ this }})
        {% endif %}
    ),
    -- Corrigindo formato do JSON para compatibilidade com BigQuery
    fix_json as (
        select
            * except (json_data),
            replace(
                replace(replace(json_data, 'None', 'null'), 'True', 'true'),
                'False',
                'false'
            ) as json_data
        from source
    ),

    -- Transformação principal dos dados extraindo informações do JSON
    transformed as (
        select
            id_reply as id_sessao,
            protocol as protocolo,
            channel as canal,
            date(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez', json_value(json_data, '$.beginDate')
                ),
                'America/Sao_Paulo'
            ) as inicio_data,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez', json_value(json_data, '$.beginDate')
                ),
                'America/Sao_Paulo'
            ) as inicio_datahora,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez', json_value(json_data, '$.endDate')
                ),
                'America/Sao_Paulo'
            ) as fim_datahora,
            json_value(json_data, '$.id') as id,

            -- Informações da URA
            struct(id_ura as id, ura_name as nome) as ura,

            -- Dados gerais do atendimento
            json_value(json_data, '$.observation') as observacao,
            json_value(json_data, '$.origin') as origem,
            json_value(json_data, '$.description') as descricao,
            json_value(json_data, '$.operator') as operador,
            json_value(json_data, '$.finalizationUser') as usuario_finalizacao,

            -- Dados de temporalidade e fluxo
            json_value(json_data, '$.firstQueuingDate') as data_primeiro_enfileiramento,
            json_value(json_data, '$.sendToOperatorDate') as data_envio_operador,
            json_value(json_data, '$.lastQueuingDate') as data_ultimo_enfileiramento,
            json_value(json_data, '$.queue') as fila,

            -- Metadados e categorizações
            json_extract(json_data, '$.tags') as tags,
            struct(
                json_extract_scalar(
                    json_extract(json_data, '$.tabulation'), '$.name'
                ) as nome,
                json_extract_scalar(
                    json_extract(json_data, '$.tabulation'), '$.id'
                ) as id
            ) as tabulacao,

            -- Informações de transferências
            json_extract_array(json_data, '$.transfers') as transferencias,

            -- Dados do contato
            struct(
                COALESCE(json_extract_scalar(
                    json_extract(json_data, '$.contact'), '$.name'
                ), dim_contato.contato_nome) as nome,
                json_extract_scalar(json_extract(json_data, '$.contact'), '$.id') as id,
                dim_contato.contato_telefone as telefone,
                dim_contato.cpf as cpf
            ) as contato,

            -- Histórico de mensagens trocadas durante o atendimento
            (
                select
                    array_agg(
                        struct(
                            -- Dados temporais da mensagem
                            datetime(
                                parse_timestamp(
                                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                                    json_extract_scalar(json_str, '$.date')
                                ),
                                'America/Sao_Paulo'
                            ) as data,

                            -- Conteúdo e metadados da mensagem
                            json_query_array(json_str, '$.attachments') as anexos,
                            json_extract_scalar(json_str, '$.hsm') as hsm,
                            safe_cast(
                                json_extract_scalar(json_str, '$.id') as string
                            ) as id,
                            json_extract_scalar(json_str, '$.source') as fonte,

                            -- Informações de mídia
                            struct(
                                json_extract_scalar(
                                    json_str, '$.media.file'
                                ) as arquivo,
                                json_extract_scalar(json_str, '$.media.name') as nome,
                                json_extract_scalar(
                                    json_str, '$.media.contentType'
                                ) as tipo_conteudo
                            ) as midia,

                            -- Conteúdo textual e classificação
                            json_extract_scalar(json_str, '$.text') as texto,
                            json_extract_scalar(json_str, '$.type') as tipo,
                            json_extract_scalar(json_str, '$.title') as titulo,
                            json_extract_scalar(json_str, '$.operator') as operador,

                            -- Informações do passo da URA
                            struct(
                                json_extract_scalar(json_str, '$.uraStep.name') as nome,
                                safe_cast(
                                    json_extract_scalar(
                                        json_str, '$.uraStep.id'
                                    ) as string
                                ) as id
                            ) as passo_ura
                        )
                        order by
                            parse_timestamp(
                                '%Y-%m-%dT%H:%M:%E*S%Ez',
                                json_extract_scalar(json_str, '$.date')
                            )
                    )
                from unnest(json_extract_array(json_data, '$.messages')) as json_str
            ) as mensagens

        from fix_json
        left join {{ ref("int_crm_whatsapp_contato") }} dim_contato 
            on dim_contato.id_contato = json_extract_scalar(json_data, '$.contact.id')
    )

-- Resultado final
select *,
    CAST(EXTRACT(YEAR FROM inicio_datahora) AS STRING) AS ano_particao,
    CAST(EXTRACT(MONTH FROM inicio_datahora) AS STRING) AS mes_particao,
    DATE(inicio_datahora) AS data_particao,
from transformed
