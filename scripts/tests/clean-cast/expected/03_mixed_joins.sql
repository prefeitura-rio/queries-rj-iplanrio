-- Test Case 3: Mixed JOINs (source + ref)
-- Expected: Apply to source columns, skip ref columns

WITH source_data AS (
    SELECT
        {{ clean_and_cast('id_source', 'string', trim=false) }} AS id_source,
        {{ clean_and_cast('nome_source', 'string', trim=false) }} AS nome
    FROM {{ source('schema', 'table_a') }}
),

ref_data AS (
    SELECT
        SAFE_CAST(REGEXP_REPLACE(id_ref, r'\.0$', '') AS STRING) AS id_ref,
        nome_ref
    FROM {{ ref('processed_model') }}
),

final AS (
    SELECT
        s.id_source,
        s.nome,
        r.id_ref,
        r.nome_ref
    FROM source_data s
    LEFT JOIN ref_data r ON s.id_source = r.id_ref
)

SELECT * FROM final
