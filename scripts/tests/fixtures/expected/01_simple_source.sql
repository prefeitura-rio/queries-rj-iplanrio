-- Test Case 1: Simple SELECT from source
-- Expected: ALL transformations should be applied

SELECT
    {{ clean_and_cast('id_col', 'string') }} AS id_col,
    {{ clean_and_cast('nome', 'string') }} AS nome,
    {{ clean_and_cast('valor', 'int64') }} AS valor,
    {{ clean_and_cast('data', 'date', trim=true) }} AS data
FROM {{ source('schema', 'table') }}
