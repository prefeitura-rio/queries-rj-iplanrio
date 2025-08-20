-- Modelo intermediário que enriquece os telefones consolidados com dados de interação do chatbot.
-- Adiciona informações sobre a última leitura, última resposta e indicador de opt-out.

with
telefones as (
    select * from {{ ref('int_telefones_raw_consolidated') }}
),

interacoes as (
    select
        contato_telefone,
        max(leitura_datahora) as ultima_leitura,
        max(resposta_datahora) as ultima_resposta
    from {{ ref('int_chatbot_base_disparo') }}
    group by contato_telefone
),

blocklist as (
    select distinct
        contato_telefone
    from {{ ref('int_crm_whatsapp_blocklist') }}
)

select
    t.*,
    i.ultima_leitura,
    i.ultima_resposta,
    -- O indicador de opt-out agora vem da tabela de blocklist
    if(b.contato_telefone is not null, 1, 0) as indicador_optout,
    -- Placeholder para a lógica de confirmação pelo app
    false as is_confirmed_by_user
from telefones t
left join interacoes i on t.telefone_numero_completo = i.contato_telefone
left join blocklist b on t.telefone_numero_completo = b.contato_telefone