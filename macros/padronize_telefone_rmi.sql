{% macro padronize_telefone_rmi(telefone_column) %}
    {% set cleaned_number %}
    regexp_replace(
        regexp_replace(
            trim({{ telefone_column }}), 
            '^0', ''
        ), 
        '[^0-9]', ''
    )
    {% endset %}
    case
        when
            length(trim({{ telefone_column }})) = 0
            or trim({{ telefone_column }}) in ('NONE', 'NULL', '0', '()', '')
            or trim({{ telefone_column }}) like '00%'
            or trim({{ telefone_column }}) like '000%'
            or trim({{ telefone_column }}) like '0000%'
            or regexp_contains(trim({{ telefone_column }}), r'^([0-9])\\1*$')
            or regexp_contains(trim({{ telefone_column }}), r'E\\+\\d+')
            or regexp_contains(trim({{ telefone_column }}), r'[a-zA-Z]')
        then null
        else
            case
                when length({{ cleaned_number }}) = 8 and substr({{ cleaned_number }}, 1, 1) in ('6', '7', '8', '9')
                then '9' || {{ cleaned_number }}
                else {{ cleaned_number }}
            end
    end
{% endmacro %}