{% macro classify_estrategia_envio(
    qualidade_numero,
    confianca_propriedade
) %}

case
    when {{ qualidade_numero }} = 'INVALIDO' then 'NÃO ENVIAR'
    when {{ confianca_propriedade }} in ('CONFIRMADA', 'MUITO_PROVAVEL') and {{ qualidade_numero }} = 'VALIDO' then 'ENVIAR'
    when {{ confianca_propriedade }} in ('CONFIRMADA', 'MUITO_PROVAVEL') and {{ qualidade_numero }} = 'SUSPEITO' then 'TESTAR'
    when {{ confianca_propriedade }} = 'PROVAVEL' and {{ qualidade_numero }} = 'VALIDO' then 'TESTAR'
    when {{ confianca_propriedade }} = 'PROVAVEL' and {{ qualidade_numero }} = 'SUSPEITO' then 'EVITAR'
    when {{ confianca_propriedade }} = 'POUCO_PROVAVEL' then 'EVITAR'
    when {{ confianca_propriedade }} = 'IMPROVAVEL' and {{ qualidade_numero }} in ('VALIDO', 'SUSPEITO') then 'EVITAR'
    else 'NÃO ENVIAR'
end

{% endmacro %}
