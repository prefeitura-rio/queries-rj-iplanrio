{{
    config(
        alias="history",
        schema='brutos_eai_logs',
        materialized="table",
        partition_by={
            "field": "last_update",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by="user_id",
    )
}}


with
    unpacked_logs as (
        -- CTE 1: Desagrupa o JSON bruto em linhas.
        select
            environment, cast(last_update as timestamp) as last_update, user_id, message
        from
            `rj-iplanrio.brutos_eai_logs_staging.history`,
            unnest(json_extract_array(messages)) as message
    ),

    parsed_logs as (
        -- CTE 2: Parseia cada linha de JSON.
        select
            environment,
            last_update,
            user_id,
            json_extract_scalar(message, '$.id') as message_id,
            cast(
                json_extract_scalar(message, '$.date') as timestamp
            ) as message_timestamp,
            json_extract_scalar(message, '$.session_id') as session_id,
            json_extract_scalar(message, '$.message_type') as message_type,
            json_extract_scalar(message, '$.step_id') as step_id,
            
            -- ALTERAÇÃO 1: Busca o conteúdo em 'content' OU em 'reasoning'
            coalesce(
                json_extract_scalar(message, '$.content'),
                json_extract_scalar(message, '$.reasoning')
            ) as message_content,

            coalesce(json_extract_scalar(message, '$.is_err'), 'false') = 'true' as is_error,
            json_extract_scalar(message, '$.status') as status,
            cast(
                json_extract_scalar(message, '$.time_since_last_message') as float64
            ) as time_since_last_message_sec,
            json_extract_scalar(message, '$.model_name') as model_name,
            json_extract_scalar(message, '$.finish_reason') as finish_reason,
            
            -- Tokens existentes
            cast(json_extract_scalar(message, '$.usage_metadata.prompt_token_count') as int64) as prompt_tokens,
            cast(json_extract_scalar(message, '$.usage_metadata.candidates_token_count') as int64) as candidates_tokens,
            cast(json_extract_scalar(message, '$.usage_metadata.total_token_count') as int64) as total_tokens,
            
            -- ALTERAÇÃO 2: Nova métrica de tokens (lida de usage_metadata OU da raiz no caso do resumo final)
            coalesce(
                cast(json_extract_scalar(message, '$.usage_metadata.thoughts_token_count') as int64),
                cast(json_extract_scalar(message, '$.thoughts_tokens') as int64)
            ) as thoughts_tokens,

            coalesce(
                json_extract_scalar(message, '$.tool_call_id'),
                json_extract_scalar(message, '$.tool_call.tool_call_id')
            ) as tool_call_id,
            coalesce(
                json_extract_scalar(message, '$.name'),
                json_extract_scalar(message, '$.tool_call.name')
            ) as tool_name,
            json_extract(message, '$.tool_call.arguments') as tool_call_arguments_json,
            json_extract(message, '$.tool_return') as tool_return_payload,
            json_extract_scalar(message, '$.stderr') as tool_stderr
        from unpacked_logs
    ),

    tool_executions as (
        -- CTE 3: Unifica execução de ferramenta.
        select
            c.environment,
            c.last_update,
            c.user_id,
            c.message_id,
            c.message_timestamp,
            c.session_id,
            'tool_execution' as message_type,
            coalesce(r.step_id, c.step_id) as step_id,
            cast(null as string) as message_content,
            coalesce(r.is_error, c.is_error) as is_error,
            coalesce(r.status, c.status) as status,
            coalesce(
                c.time_since_last_message_sec, r.time_since_last_message_sec
            ) as time_since_last_message_sec,
            coalesce(r.model_name, c.model_name) as model_name,
            coalesce(r.finish_reason, c.finish_reason) as finish_reason,
            coalesce(c.prompt_tokens, 0) + coalesce(r.prompt_tokens, 0) as prompt_tokens,
            coalesce(c.candidates_tokens, 0) + coalesce(r.candidates_tokens, 0) as candidates_tokens,
            coalesce(c.total_tokens, 0) + coalesce(r.total_tokens, 0) as total_tokens,
            
            -- ALTERAÇÃO 3: Adiciona a soma de thoughts_tokens para ferramentas (geralmente é 0, mas mantém consistência)
            coalesce(c.thoughts_tokens, 0) + coalesce(r.thoughts_tokens, 0) as thoughts_tokens,

            c.tool_call_id,
            coalesce(r.tool_name, c.tool_name) as tool_name,
            c.tool_call_arguments_json,
            r.tool_return_payload,
            r.tool_stderr
        from (select * from parsed_logs where message_type = 'tool_call_message') as c
        left join
            (select * from parsed_logs where message_type = 'tool_return_message') as r
            on c.session_id = r.session_id
            and c.tool_call_id = r.tool_call_id
    ),

    other_messages as (
        -- CTE 4: Isola outras mensagens (user, assistant, reasoning, system, usage_statistics).
        select
            environment,
            last_update,
            user_id,
            message_id,
            message_timestamp,
            session_id,
            message_type,
            step_id,
            message_content,
            is_error,
            status,
            time_since_last_message_sec,
            model_name,
            finish_reason,
            prompt_tokens,
            candidates_tokens,
            total_tokens,
            
            -- ALTERAÇÃO 4: Inclui a coluna na seleção final
            thoughts_tokens,

            tool_call_id,
            tool_name,
            tool_call_arguments_json,
            tool_return_payload,
            tool_stderr
        from parsed_logs
        where message_type not in ('tool_call_message', 'tool_return_message')
    )

-- Passo Final: União
select *
from tool_executions
union all
select *
from other_messages
{# where message_timestamp is not null #}
{# order by user_id, message_timestamp #}