{% macro clean_and_cast(column_name, data_type='string', trim=true, safe=true) %}
{#
    Macro para limpar valores numéricos com sufixo .0 e fazer cast para o tipo desejado.

    Parâmetros:
        column_name (string): Nome da coluna a ser transformada
        data_type (string): Tipo de destino (string, int64, etc.). Default: 'string'
        trim (boolean): Se deve aplicar TRIM antes do REGEXP_REPLACE. Default: true
        safe (boolean): Se deve usar SAFE_CAST ao invés de CAST. Default: true

    Exemplos:
        {{ clean_and_cast('cpf') }}
        -- Resultado: SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(cpf AS STRING)), r'\.0$', '') AS string)

        {{ clean_and_cast('cpf', 'string', trim=false) }}
        -- Resultado: SAFE_CAST(REGEXP_REPLACE(CAST(cpf AS STRING), r'\.0$', '') AS string)

        {{ clean_and_cast('id_funcionario', 'int64') }}
        -- Resultado: SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(id_funcionario AS STRING)), r'\.0$', '') AS int64)

        {{ clean_and_cast('valor', 'string', safe=false) }}
        -- Resultado: CAST(REGEXP_REPLACE(TRIM(CAST(valor AS STRING)), r'\.0$', '') AS string)
#}
    {%- set cast_function = 'SAFE_CAST' if safe else 'CAST' -%}
    {%- if trim -%}
        {{ cast_function }}(REGEXP_REPLACE(TRIM(CAST({{ column_name }} AS STRING)), r'\.0$', '') AS {{ data_type }})
    {%- else -%}
        {{ cast_function }}(REGEXP_REPLACE(CAST({{ column_name }} AS STRING), r'\.0$', '') AS {{ data_type }})
    {%- endif -%}
{% endmacro %}
