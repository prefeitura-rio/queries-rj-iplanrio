-- Test Case 7: Complex multi-line transformations
-- Expected: Should handle multi-line SAFE_CAST patterns

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(
            id_col,
            r'\.0$',
            ''
        ) AS STRING
    ) AS id_multi,

    SAFE_CAST(
        REGEXP_REPLACE(
            TRIM(CAST(valor AS STRING)),
            r'\.0$',
            ''
        ) AS INT64
    ) AS valor_multi,

    nome
FROM {{ source('dataset', 'complex_table') }}
