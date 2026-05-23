-- Traduz a sigla de tipo de missão para o nome completo em português.
-- Siglas conhecidas: PB, PTR, RF, DS, SV, SP.
-- Retorna NULL para siglas não mapeadas.
--
-- Uso:
-- {{ tipo_missao_nome("tipo_missao") }} as tipo_missao_nome
{% macro tipo_missao_nome(column) %}
    case
        upper(trim({{ column }}))
        when 'PB'
        then 'posto'
        when 'PTR'
        then 'patrulhamento'
        when 'RF'
        then 'refeicao'
        when 'DS'
        then 'deslocamento'
        when 'SV'
        then 'supervisao'
        when 'SP'
        then 'suporte'
    end
{%- endmacro %}