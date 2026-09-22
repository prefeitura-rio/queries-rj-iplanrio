-- Test Case 3: Mixed JOINs (source + ref)
-- Expected: Apply to source columns, skip ref columns

WITH source_data AS (
    SELECT
        SAFE_CAST(REGEXP_REPLACE(id_source, r'\.0$', '') AS STRING) AS id_source,
        SAFE_CAST(REGEXP_REPLACE(nome_source, r'\.0$', '') AS STRING) AS nome
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
