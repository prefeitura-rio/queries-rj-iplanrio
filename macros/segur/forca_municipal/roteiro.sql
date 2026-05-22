-- Padroniza o roteiro removendo prefixos operacionais e redundâncias textuais.
--
-- Strip em 3 passos:
--   1. Prefixo de tipo + número opcional + tab
--      (DESLOCAMENTO, SUPERVISÃO, RF, PTR, PB, SV, SP — ex: "PTR _01\t", "DESLOCAMENTO\t  ")
--   2. "Dentro da Subárea" — prefixo de redundância das subareas
--   3. "Dentro da área da Base" — prefixo de redundância das areas
--
-- Uso:
--   {{ padronize_roteiro("json_value(doc, '$.roteiro')") }} as roteiro
--
-- Nota: roteiro_raw (valor bruto) deve ser persistido separadamente para uso
-- no roteiro_norm (normalização para tipo_operacional) e para rastreabilidade.
{% macro padronize_roteiro(expr) %}
trim(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                trim({{ expr }}),
                r'^(?:DESLOCAMENTO|SUPERVISÃO|RF|PTR|PB|SV|SP)(?:\s*_\d+)?\t\s*',
                ''
            ),
            r'(?i)^Dentro da Sub[aá]rea\s*',
            ''
        ),
        r'(?i)^Dentro da [aá]rea da Base\s*',
        ''
    )
)
{%- endmacro %}
