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
        sql_header = """
        create temp function calculate_24h_fixed_anchors(
            inputs array<struct<msg_order int64, ts_ms int64>>
        )
        returns array<struct<msg_idx int64, anchor_ts int64, session_num int64>>
        language js as r'''
            if (!inputs || inputs.length === 0) return [];

            const results = [];
            // 24 horas em milissegundos
            const TWENTY_FOUR_HOURS_MS = 86400000;

            // Inicializa com o primeiro item
            // Importante: Forçar conversão para Number para evitar problemas com tipos do BQ
            let currentAnchor = Number(inputs[0].ts_ms);
            let sessionNum = 1;

            for (let i = 0; i < inputs.length; i++) {
                const item = inputs[i];
                const ts = Number(item.ts_ms);

                // Lógica de Janela Fixa:
                // Se a mensagem atual passou de 24h DO ANCHOR ATUAL, cria novo anchor.
                // Caso contrário, mantém o anchor antigo (sessão dura 24h exatas).
                if (ts > currentAnchor + TWENTY_FOUR_HOURS_MS) {
                    currentAnchor = ts;
                    sessionNum++;
                }

                results.push({
                    msg_idx: item.msg_order, // Retorna o ID original para o JOIN
                    anchor_ts: Math.floor(currentAnchor), // Garante inteiro
                    session_num: sessionNum
                });
            }

            return results;
        ''';
        """
    )
}}

-- =============================================================================
-- DOCUMENTAÇÃO DOS SESSION IDs
-- =============================================================================
--
-- Este modelo calcula 3 tipos diferentes de session_id para análise de conversas:
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │ 1. SESSION_ID_1H_MOVEL (Janela Móvel de 1 hora)                             │
-- ├─────────────────────────────────────────────────────────────────────────────┤
-- │ Lógica: Nova sessão inicia quando o GAP entre duas mensagens consecutivas  │
-- │         é MAIOR que 1 hora (3600 segundos).                                 │
-- │                                                                             │
-- │ Critério: gap_entre_msgs > 3600s → nova sessão                              │
-- │                                                                             │
-- │ Exemplo:                                                                    │
-- │ ┌──────────┬─────────────────────┬───────────────────┬────────┐             │
-- │ │ Mensagem │ Timestamp           │ Gap (desde anterior) │ Sessão │          │
-- │ ├──────────┼─────────────────────┼───────────────────┼────────┤             │
-- │ │ msg1     │ 2025-01-01 10:00:00 │ -                 │ S1     │             │
-- │ │ msg2     │ 2025-01-01 10:30:00 │ 30 min            │ S1     │ (gap < 1h)  │
-- │ │ msg3     │ 2025-01-01 11:45:00 │ 1h15min           │ S2     │ (gap > 1h)  │
-- │ │ msg4     │ 2025-01-01 12:00:00 │ 15 min            │ S2     │ (gap < 1h)  │
-- │ └──────────┴─────────────────────┴───────────────────┴────────┘             │
-- └─────────────────────────────────────────────────────────────────────────────┘
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │ 2. SESSION_ID_24H_MOVEL (Janela Móvel de 24 horas)                          │
-- ├─────────────────────────────────────────────────────────────────────────────┤
-- │ Lógica: Nova sessão inicia quando o GAP entre duas mensagens consecutivas  │
-- │         é MAIOR que 24 horas (86400 segundos).                              │
-- │                                                                             │
-- │ Critério: gap_entre_msgs > 86400s → nova sessão                             │
-- │                                                                             │
-- │ Exemplo:                                                                    │
-- │ ┌──────────┬─────────────────────┬───────────────────┬────────┐             │
-- │ │ Mensagem │ Timestamp           │ Gap (desde anterior) │ Sessão │          │
-- │ ├──────────┼─────────────────────┼───────────────────┼────────┤             │
-- │ │ msg1     │ 2025-01-01 10:00:00 │ -                 │ S1     │             │
-- │ │ msg2     │ 2025-01-01 22:00:00 │ 12h               │ S1     │ (gap < 24h) │
-- │ │ msg3     │ 2025-01-03 10:00:00 │ 36h               │ S2     │ (gap > 24h) │
-- │ │ msg4     │ 2025-01-03 15:00:00 │ 5h                │ S2     │ (gap < 24h) │
-- │ └──────────┴─────────────────────┴───────────────────┴────────┘             │
-- └─────────────────────────────────────────────────────────────────────────────┘
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │ 3. SESSION_ID_24H_FIXO (Janela Fixa de 24 horas)                            │
-- ├─────────────────────────────────────────────────────────────────────────────┤
-- │ Lógica: Sessão tem duração EXATA de 24 horas a partir do "anchor"           │
-- │         (primeira mensagem da sessão). Nova sessão inicia quando uma        │
-- │         mensagem ultrapassa 24h do anchor, INDEPENDENTE do gap.             │
-- │                                                                             │
-- │ Critério: (timestamp_msg - anchor) > 24h → nova sessão (novo anchor)        │
-- │                                                                             │
-- │ Exemplo:                                                                    │
-- │ ┌──────────┬─────────────────────┬────────────────────┬────────┬───────────┐│
-- │ │ Mensagem │ Timestamp           │ Tempo desde anchor │ Sessão │ Anchor    ││
-- │ ├──────────┼─────────────────────┼────────────────────┼────────┼───────────┤│
-- │ │ msg1     │ 2025-01-01 10:00:00 │ 0h (é o anchor)    │ S1     │ 01 10:00  ││
-- │ │ msg2     │ 2025-01-01 22:00:00 │ 12h                │ S1     │ 01 10:00  ││
-- │ │ msg3     │ 2025-01-02 08:00:00 │ 22h                │ S1     │ 01 10:00  ││
-- │ │ msg4     │ 2025-01-02 11:00:00 │ 25h (> 24h)        │ S2     │ 02 11:00  ││
-- │ │ msg5     │ 2025-01-02 15:00:00 │ 4h do novo anchor  │ S2     │ 02 11:00  ││
-- │ └──────────┴─────────────────────┴────────────────────┴────────┴───────────┘│
-- │                                                                             │
-- │ DIFERENÇA CHAVE entre MÓVEL e FIXO:                                         │
-- │ - MÓVEL: Olha o GAP entre mensagens CONSECUTIVAS                            │
-- │ - FIXO:  Olha o TEMPO TOTAL desde o INÍCIO (anchor) da sessão               │
-- │                                                                             │
-- │ Caso de uso FIXO: Análise de "dias de uso" - mesmo que usuário mande        │
-- │ mensagens a cada 1h durante 3 dias, terá 3 sessões (uma por dia).           │
-- └─────────────────────────────────────────────────────────────────────────────┘
--
-- =============================================================================
-- ESTRUTURA DOS CAMPOS DE SESSÃO NO OUTPUT
-- =============================================================================
--
-- session_id               → ID original do sistema (vem do JSON 1h movel)
-- session_start_1h         → Timestamp do início da sessão (1h móvel)
-- session_id_1h_movel      → Hash MD5 truncado (16 chars) identificador único
-- session_start_24h_movel  → Timestamp do início da sessão (24h móvel)
-- session_id_24h_movel     → Hash MD5 truncado (16 chars) identificador único
-- session_start_24h_fixo   → Timestamp do anchor da sessão (24h fixo)
-- session_id_24h_fixo      → Hash MD5 truncado (16 chars) identificador único
--
-- Fórmula do hash: substr(to_hex(md5(concat(session_start, '_', user_id))), 1, 16)
--
-- =============================================================================
-- UDF JavaScript para calcular session anchors de 24h fixo
-- =============================================================================
-- Recebe: array de STRUCTS {msg_order, ts_ms} ordenado por msg_order
-- Retorna: array de STRUCTS {msg_idx, anchor_ts, session_num}
-- Garante alinhamento perfeito e tipagem numérica com Number() e Math.floor()
-- =============================================================================

with
    unpacked_logs as (
        -- CTE 1: Desagrupa o JSON bruto em linhas.
        select
            environment,
            cast(last_update as timestamp) as last_update,
            user_id,
            message
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
            cast(json_extract_scalar(message, '$.usage_metadata.prompt_token_count') as int64) as prompt_tokens,
            cast(json_extract_scalar(message, '$.usage_metadata.candidates_token_count') as int64) as candidates_tokens,
            cast(json_extract_scalar(message, '$.usage_metadata.total_token_count') as int64) as total_tokens,
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
            thoughts_tokens,
            tool_call_id,
            tool_name,
            tool_call_arguments_json,
            tool_return_payload,
            tool_stderr
        from parsed_logs
        where message_type not in ('tool_call_message', 'tool_return_message')
    ),

    -- União de todas as mensagens
    all_messages as (
        select * from tool_executions
        union all
        select * from other_messages
    ),

    -- =============================================================================
    -- CÁLCULO DAS SESSÕES MÓVEIS (1H e 24H)
    -- =============================================================================
    -- Método: Window Functions com LAG + SUM cumulativo
    --
    -- Passo 1: ordered_messages
    --   - Ordena mensagens por (environment, user_id, timestamp)
    --   - Calcula prev_message_timestamp com LAG()
    --
    -- Passo 2: with_session_groups
    --   - Compara timestamp atual com anterior
    --   - Se gap > threshold → marca 1 (nova sessão), senão 0
    --   - SUM cumulativo cria o session_group (1, 2, 3...)
    --
    -- Passo 3: with_session_starts
    --   - FIRST_VALUE() pega o timestamp da primeira msg de cada session_group
    --   - Este timestamp vira o session_start
    -- =============================================================================

    ordered_messages as (
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
            thoughts_tokens,
            tool_call_id,
            tool_name,
            tool_call_arguments_json,
            tool_return_payload,
            tool_stderr,
            row_number() over (
                partition by environment, user_id
                order by message_timestamp asc, message_id asc
            ) as msg_order,
            lag(message_timestamp) over (
                partition by environment, user_id
                order by message_timestamp asc, message_id asc
            ) as prev_message_timestamp
        from all_messages
        where message_timestamp is not null
    ),

    with_session_groups as (
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
            thoughts_tokens,
            tool_call_id,
            tool_name,
            tool_call_arguments_json,
            tool_return_payload,
            tool_stderr,
            msg_order,

            -- =========================================================
            -- SESSION_GROUP_1H: Janela móvel de 1 hora (3600 segundos)
            -- =========================================================
            -- Lógica: Marca 1 quando gap > 1h, senão 0
            -- SUM cumulativo agrupa em sessões (1, 2, 3, ...)
            sum(
                case
                    when prev_message_timestamp is null then 1  -- Primeira msg = nova sessão
                    when timestamp_diff(message_timestamp, prev_message_timestamp, second) > 3600 then 1  -- Gap > 1h = nova sessão
                    else 0  -- Continua na mesma sessão
                end
            ) over (
                partition by environment, user_id
                order by msg_order
                rows unbounded preceding
            ) as session_group_1h,

            -- =========================================================
            -- SESSION_GROUP_24H_MOVEL: Janela móvel de 24h (86400 seg)
            -- =========================================================
            -- Lógica: Marca 1 quando gap > 24h, senão 0
            -- SUM cumulativo agrupa em sessões (1, 2, 3, ...)
            sum(
                case
                    when prev_message_timestamp is null then 1  -- Primeira msg = nova sessão
                    when timestamp_diff(message_timestamp, prev_message_timestamp, second) > 86400 then 1  -- Gap > 24h = nova sessão
                    else 0  -- Continua na mesma sessão
                end
            ) over (
                partition by environment, user_id
                order by msg_order
                rows unbounded preceding
            ) as session_group_24h_movel

        from ordered_messages
    ),

    with_session_starts as (
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
            thoughts_tokens,
            tool_call_id,
            tool_name,
            tool_call_arguments_json,
            tool_return_payload,
            tool_stderr,
            msg_order,
            session_group_1h,
            session_group_24h_movel,

            -- session_start = timestamp da PRIMEIRA mensagem de cada session_group
            -- FIRST_VALUE() pega o menor timestamp dentro de cada partição
            first_value(message_timestamp) over (
                partition by environment, user_id, session_group_1h
                order by msg_order
            ) as session_start_1h,

            first_value(message_timestamp) over (
                partition by environment, user_id, session_group_24h_movel
                order by msg_order
            ) as session_start_24h_movel

        from with_session_groups
    ),

    -- =============================================================================
    -- CÁLCULO DA SESSÃO 24H FIXA
    -- =============================================================================
    -- Método: JavaScript UDF (necessário para manter estado do anchor)
    --
    -- Por que UDF? Window functions não conseguem implementar a lógica de
    -- "anchor fixo" porque precisamos comparar SEMPRE com o anchor, não com
    -- a mensagem anterior. O anchor só muda quando ultrapassamos 24h dele.
    --
    -- Passo 1: user_messages_packed
    --   - Agrupa todas as mensagens de cada (environment, user_id) em um array
    --   - Cada elemento é um STRUCT{msg_order, ts_ms}
    --
    -- Passo 2: calculate_24h_fixed_anchors (UDF)
    --   - Itera pelo array ordenado
    --   - Mantém currentAnchor = timestamp da primeira msg da sessão
    --   - Se (ts_atual - currentAnchor) > 24h → novo anchor = ts_atual
    --   - Retorna {msg_idx, anchor_ts, session_num} para cada mensagem
    --
    -- Passo 3: session_24h_calculated
    --   - UNNEST do resultado da UDF
    --   - Converte anchor_ts (millis) de volta para TIMESTAMP
    -- =============================================================================

    user_messages_packed as (
        select
            environment,
            user_id,
            array_agg(
                struct(msg_order, unix_millis(message_timestamp) as ts_ms)
                order by msg_order
            ) as input_data
        from with_session_starts
        group by environment, user_id
    ),

    session_24h_calculated as (
        select
            environment,
            user_id,
            output.msg_idx,
            timestamp_millis(output.anchor_ts) as session_start_24h_fixo,
            output.session_num as session_number_24h
        from user_messages_packed,
        unnest(calculate_24h_fixed_anchors(input_data)) as output
    ),

    -- =============================================================================
    -- RESULTADO FINAL
    -- =============================================================================
    -- JOIN entre with_session_starts (sessões móveis) e session_24h_calculated
    -- (sessão fixa) usando msg_order como chave de alinhamento.
    --
    -- Campos de saída organizados em grupos:
    -- 1. Identificadores (environment, last_update, user_id, message_id, timestamp)
    -- 2. Session IDs (original, 1h_movel, 24h_movel, 24h_fixo)
    -- 3. Dados da mensagem (type, step_id, content, is_error, status, etc)
    -- 4. Modelo e tokens (model_name, finish_reason, prompt/candidates/total/thoughts)
    -- 5. Ferramentas (tool_call_id, tool_name, arguments, return, stderr)
    -- =============================================================================

    final as (
        select
            -- Identificadores
            s.environment,
            s.last_update,
            s.user_id,
            s.message_id,
            s.message_timestamp,

            -- Session IDs (agrupados)
            s.session_id as session_id,
            -- 1h móvel
            s.session_start_1h,
            substr(to_hex(md5(concat(
                cast(s.session_start_1h as string), '_', coalesce(s.user_id, 'unknown')
            ))), 1, 16) as session_id_1h_movel,
            -- 24h móvel
            s.session_start_24h_movel,
            substr(to_hex(md5(concat(
                cast(s.session_start_24h_movel as string), '_', coalesce(s.user_id, 'unknown')
            ))), 1, 16) as session_id_24h_movel,
            -- 24h fixo
            r.session_start_24h_fixo,
            substr(to_hex(md5(concat(
                cast(r.session_start_24h_fixo as string), '_', coalesce(s.user_id, 'unknown')
            ))), 1, 16) as session_id_24h_fixo,

            -- Dados da mensagem
            s.message_type,
            s.step_id,
            s.message_content,
            s.is_error,
            s.status,
            s.time_since_last_message_sec,

            -- Modelo e tokens
            s.model_name,
            s.finish_reason,
            s.prompt_tokens,
            s.candidates_tokens,
            s.total_tokens,
            s.thoughts_tokens,

            -- Ferramentas
            s.tool_call_id,
            s.tool_name,
            s.tool_call_arguments_json,
            s.tool_return_payload,
            s.tool_stderr

        from with_session_starts s
        inner join session_24h_calculated r
            on ifnull(s.environment, 'NULL_ENV') = ifnull(r.environment, 'NULL_ENV')
            and s.user_id = r.user_id
            and s.msg_order = r.msg_idx
    )

select * from final
