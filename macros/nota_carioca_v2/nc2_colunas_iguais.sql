{#
    Teste genérico: o modelo tem exatamente as mesmas colunas (nome, tipo e posição) da fonte de referência.
    A referência é nc2_fonte(fonte, tabela): em prod e pr_prod é a MVT atual do nota_carioca (a exportação para o
    Oracle e o carregador do Postgres leem as colunas por posição); em dev e pr é a seed sintética que repete o
    layout dessa MVT. Devolve uma linha por posição divergente (nenhuma linha = passa).
#}
{% test nc2_colunas_iguais(model, fonte, tabela) %}
    {#- fora do if execute, para o dbt registrar a dependência da seed (ou da source) já no parse -#}
    {%- set referencia = nc2_fonte_relation(fonte, tabela) -%}
    {%- if execute -%}
        {%- set atuais = adapter.get_columns_in_relation(model) -%}
        {%- set esperadas = adapter.get_columns_in_relation(referencia) -%}
        {%- set total = [atuais | length, esperadas | length] | max -%}
        {%- set divergencias = [] -%}
        {%- for i in range(total) -%}
            {%- set atual = atuais[i] if i < (atuais | length) else none -%}
            {%- set esperada = esperadas[i] if i < (esperadas | length) else none -%}
            {%- set nome_atual = (atual.name | lower) if atual else "(ausente)" -%}
            {%- set nome_esperado = (
                (esperada.name | lower) if esperada else "(ausente)"
            ) -%}
            {%- set tipo_atual = (atual.data_type | upper) if atual else "(ausente)" -%}
            {%- set tipo_esperado = (
                (esperada.data_type | upper) if esperada else "(ausente)"
            ) -%}
            {%- if nome_atual != nome_esperado or tipo_atual != tipo_esperado -%}
                {%- do divergencias.append(
                    "select "
                    ~ (i + 1)
                    ~ " as posicao, '"
                    ~ nome_atual
                    ~ " "
                    ~ tipo_atual
                    ~ "' as modelo, '"
                    ~ nome_esperado
                    ~ " "
                    ~ tipo_esperado
                    ~ "' as referencia"
                ) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if divergencias | length > 0 -%} {{ divergencias | join(" union all ") }}
        {%- else -%}
            select 0 as posicao, '' as modelo, '' as referencia
            from (select 1)
            where false
        {%- endif -%}
    {%- else -%}
        select 0 as posicao, '' as modelo, '' as referencia from (select 1) where false
    {%- endif -%}
{% endtest %}
