-- Test Case 13: Nested queries with multiple levels
-- Expected: Apply transformations respecting each level's source

WITH base AS (
    SELECT
        {{ clean_and_cast('id_col', 'string', trim=false) }} AS id_base,
        nome,
        valor
    FROM {{ source('dataset', 'base_table') }}
),

processed AS (
    SELECT
        b.id_base,
        b.nome,
        b.valor,
        (
            SELECT MAX(SAFE_CAST(REGEXP_REPLACE(sub_val, r'\.0$', '') AS INT64))
            FROM {{ ref('sub_table') }} s
            WHERE s.id_base = b.id_base
        ) AS max_sub_valor
    FROM base b
),

final AS (
    SELECT
        p.id_base,
        p.nome,
        p.valor,
        p.max_sub_valor,
        (
            SELECT COUNT(*)
            FROM {{ source('dataset', 'details') }} d
            WHERE {{ clean_and_cast('id_ref', 'string', trim=false) }} = p.id_base
        ) AS detail_count
    FROM processed p
)

SELECT * FROM final
