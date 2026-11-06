{#
    Gera os "radicais" sintáticos candidatos de um número de documento (NF), para
    permitir o match entre o número declarado e o número extraído pela IA mesmo
    quando há formatação divergente.

    Retorna uma expressão ARRAY<STRING> (valores distintos, sem nulos/vazios e sem
    "apenas ano"). O match entre dois números ocorre quando há interseção entre
    os arrays de ambos.

    Variações geradas (todas com zeros à esquerda removidos):
      A. Apenas dígitos                       -> '2025/000123'  => '2025000123'
      B. Sem padrão Nota Carioca (ano+zeros)  -> '2025.000123'  => '123'
      C. Sem competência/ano no final         -> '4104052025'   => '4104'
      D. Pedaço 0 após split por / - _ espaço -> '123/2025'     => '123'
      E. Pedaço 1 após split por / - _ espaço -> '2025/123'     => '123'

    Uso:
        {{ tratar_numero_documento('od.numero_documento') }}
#}
{% macro tratar_numero_documento(column) %}
    {%- set s = "cast(" ~ column ~ " as string)" -%}
    {%- set s_sem_ponto = "regexp_replace(" ~ s ~ ", r'\\.', '')" -%}
    {%- set partes = "split(regexp_replace(" ~ s ~ ", r'[/_\\-\\s]+', '#'), '#')" -%}
    {%- set ano_inicio = "r'^(20[12][0-9])0+'" -%}
    array(
        select distinct c
        from
            unnest(
                [
                    -- A. Radical limpo de pontuação e zeros
                    ltrim(regexp_replace({{ s }}, r'\D', ''), '0'),

                    -- B. Remoção do padrão Nota Carioca (ano 2010-2029 + zeros à esquerda)
                    ltrim(regexp_replace({{ s_sem_ponto }}, {{ ano_inicio }}, ''), '0'),

                    -- C. Remoção de competência/ano concatenado no final, sem barra
                    ltrim(regexp_replace({{ s_sem_ponto }}, r'(0[1-9]|1[0-2])(20[12][0-9])$', ''), '0'),
                    ltrim(regexp_replace({{ s_sem_ponto }}, r'(20[12][0-9])$', ''), '0'),

                    -- D. Pedaço 0 ao separar por barra, hífen, underline ou espaço
                    ltrim(regexp_replace({{ partes }}[safe_offset(0)], r'\D', ''), '0'),
                    ltrim(
                        regexp_replace(
                            regexp_replace({{ partes }}[safe_offset(0)], r'\.', ''),
                            {{ ano_inicio }}, ''
                        ),
                        '0'
                    ),

                    -- E. Pedaço 1 ao separar por barra, hífen, underline ou espaço
                    ltrim(regexp_replace({{ partes }}[safe_offset(1)], r'\D', ''), '0'),
                    ltrim(
                        regexp_replace(
                            regexp_replace({{ partes }}[safe_offset(1)], r'\.', ''),
                            {{ ano_inicio }}, ''
                        ),
                        '0'
                    )
                ]
            ) as c
        where
            c is not null
            and c != ''
            -- Descarta o ano isolado
            and c not in ('2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026')
    )
{% endmacro %}
