-- Converte uma string em slug: lowercase, sem acentos, caracteres não-alfanuméricos
-- substituídos por underscore, underscores duplos colapsados, trim de bordas.
--
-- Uso:
--   {{ slugify("roteiro") }} as id_roteiro
--
-- Exemplos:
--   "Litorânea"                             → litoranea
--   "Jardim de Alah"                        → jardim_de_alah
--   "Av. Lauro Sodré, da Rua General..."    → av_lauro_sodre_da_rua_general
{% macro slugify(expr) %}
trim(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                lower(normalize({{ expr }}, nfd)),
                r'\pM',
                ''
            ),
            r'[^a-z0-9]+',
            '_'
        ),
        r'^_|_$',
        ''
    )
)
{%- endmacro %}
