-- Test Case 1: Simple SELECT from source
-- Expected: ALL transformations should be applied

SELECT
    {{ clean_and_cast('id_col', 'string', trim=false) }} AS id_col,
    {{ clean_and_cast('nome', 'string', trim=false) }} AS nome,
    {{ clean_and_cast('valor', 'int64', trim=false) }} AS valor,
    {{ clean_and_cast('data', 'date') }} AS data
FROM {{ source('schema', 'table') }}
