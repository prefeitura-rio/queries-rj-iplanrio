{% macro save_query_results(query, output_file) %}
  {% set results = run_query(query) %}
  {% if results %}
    {% for row in results %}
      {{ log(row, info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}