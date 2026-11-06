{#
    Resolve uma fonte da trilha nota_carioca_v2 (raw do parser, réplicas do Oracle e o lado Oracle da paridade).

    - dev e pr: seed sintética versionada em seeds/nota_carioca_v2/, chamada nc2_seed__<fonte>__<tabela em minúsculas>.
      Dados fictícios, sem PII: o CI de PR exercita modelos, testes e unit tests sem depender de dados reais.
    - prod e pr_prod: source() real (rj-iplanrio-dia). Esses targets só enxergam os modelos nota_carioca_v2 com
      var('nota_carioca_v2_habilitado') = true, porque as fontes raw ainda não existem em produção.

    Devolve a Relation (use nc2_fonte para o texto SQL).
#}
{% macro nc2_fonte_relation(fonte, tabela) -%}
    {%- if target.name in ["dev", "pr"] -%}
        {{ return(ref("nc2_seed__" ~ fonte ~ "__" ~ (tabela | lower))) }}
    {%- else -%} {{ return(source(fonte, tabela)) }}
    {%- endif -%}
{%- endmacro %}


{% macro nc2_fonte(fonte, tabela) -%}
    {{ nc2_fonte_relation(fonte, tabela) }}
{%- endmacro %}
