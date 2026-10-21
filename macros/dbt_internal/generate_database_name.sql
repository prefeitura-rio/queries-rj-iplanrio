{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- if target.name != 'prod' and target.name != 'pr_prod' -%}
        rj-iplanrio-dev
    {%- else -%}
        {%- set default_database = target.database -%}
        {%- if custom_database_name is none -%}

            {{ default_database }}

        {%- else -%}

            {{ custom_database_name | trim }}

        {%- endif -%}
    {%- endif -%}

{%- endmacro %}