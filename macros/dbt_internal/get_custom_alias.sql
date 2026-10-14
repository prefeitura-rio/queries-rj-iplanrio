{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {# TODO: apenas local, remover antes de ir para master! #}
    {%- if target.name == "pr" or target.name == "dev_cgm_triagem_nf" -%}

        {{ node.name }}

    {%- elif custom_alias_name -%}

        {{ custom_alias_name | trim }}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
