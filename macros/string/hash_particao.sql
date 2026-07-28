{% macro hash_particao(column) %}
    mod(abs(farm_fingerprint({{ column }})), 100000000000000)
{% endmacro %}
