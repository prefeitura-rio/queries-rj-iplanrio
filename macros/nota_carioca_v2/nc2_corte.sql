{#
    Corte de NSU da paridade F1 (paridade_nc2__corte): só entra na comparação o que o Oracle já pode ter extraído, isto é,
    NSU menor ou igual ao maior NSU do Oracle. Sem o corte, o que o ADN já entregou à nuvem e o Oracle ainda não leu
    apareceria como divergência.
#}
{% macro nc2_corte() -%}
    (select corte_nsu from {{ ref("paridade_nc2__corte") }})
{%- endmacro %}
