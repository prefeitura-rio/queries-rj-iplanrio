-- Test Case 7: Complex multi-line transformations
-- Expected: Should handle multi-line SAFE_CAST patterns

SELECT
    {{ clean_and_cast('id_col', 'string') }} AS id_multi,

    {{ clean_and_cast('valor', 'int64', trim=true) }} AS valor_multi,

    nome
FROM {{ source('dataset', 'complex_table') }}
