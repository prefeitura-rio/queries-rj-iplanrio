{% macro generate_schema_name(custom_schema_name, node) -%}

    {% if target.name == "dev" -%}

        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}

            {{ var("DBT_USER") }}__{{ default_schema }}

        {%- else -%} {{ var("DBT_USER") }}__{{ custom_schema_name | trim }}

        {%- endif -%}

    {%- elif target.name == "pr" -%}	
        {%- set default_schema = target.schema -%}

            {{default_schema}}
    {%- else %}

        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%} {{ default_schema }}

        {%- else -%} {{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}
{%- endmacro %}
