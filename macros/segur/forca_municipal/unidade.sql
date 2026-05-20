-- Extrai o tipo da unidade a partir do prefixo alfabético do id_unidade.
-- Ex.: "POG01-LITORANEA" → "POG" | "MOT07-NORTE" → "MOT"
{% macro tipo_unidade(column) %}
    regexp_extract(upper(trim(safe_cast({{ column }} as string))), r'^([A-Z]+)\d')
{% endmacro %}

-- Extrai a base operacional a partir do sufixo após o hífen do id_unidade.
-- Ex.: "POG01-LITORANEA" → "LITORANEA" | "MOT07-NORTE" → "NORTE"
-- Retorna NULL quando não há sufixo (unidade sem base definida).
{% macro base_operacional(column) %}
    regexp_extract(upper(trim(safe_cast({{ column }} as string))), r'-(.+)$')
{% endmacro %}
