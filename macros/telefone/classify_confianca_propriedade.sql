{% macro classify_confianca_propriedade(
    source_of_data,
    last_update_date,
    last_reply_date,
    is_opt_out,
    is_confirmed_by_user
) %}

case
    -- CONFIRMADA: O usuário validou o número no aplicativo oficial.
    -- A lógica para `is_confirmed_by_user` precisa ser implementada na fonte.
    when {{ is_confirmed_by_user }} = true then 'CONFIRMADA'

    -- IMPROVAVEL: Falha confirmada ou opt-out.
    when {{ is_opt_out }} = 1 then 'IMPROVAVEL'

    -- MUITO_PROVAVEL: O usuário respondeu a uma mensagem do WhatsApp nos últimos 6 meses.
    when {{ last_reply_date }} >= date_sub(current_date(), interval 6 month) then 'MUITO_PROVAVEL'

    -- PROVAVEL: Atualização recente (últimos 6 meses) em uma fonte de alta confiança.
    when {{ source_of_data }} in ('sms', 'bcadastro') and {{ last_update_date }} >= date_sub(current_date(), interval 6 month) then 'PROVAVEL'

    -- POUCO_PROVAVEL: Atualização nos últimos 2 anos em qualquer sistema oficial, ou sem data de atualização.
    when {{ last_update_date }} >= date_sub(current_date(), interval 2 year) then 'POUCO_PROVAVEL'
    when {{ last_update_date }} is null then 'POUCO_PROVAVEL'

    -- IMPROVAVEL: Dados com mais de 2 anos.
    when {{ last_update_date }} < date_sub(current_date(), interval 2 year) then 'IMPROVAVEL'

    else 'IMPROVAVEL' -- O padrão é improvável
end

{% endmacro %}
