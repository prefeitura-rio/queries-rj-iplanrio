-- Test Case 8: Real case - Multiple CTEs with JOINs
-- Based on: models/mart/sma/ergon_saude/mart_ergon_saude_funcionarios.sql
-- Expected: Apply transformations only in source CTE, skip in ref CTEs

WITH
    funcionarios AS (
        SELECT
            SAFE_CAST(REGEXP_REPLACE(id_func, r'\.0$', '') AS STRING) AS id_funcionario,
            SAFE_CAST(REGEXP_REPLACE(cpf, r'\.0$', '') AS STRING) AS cpf,
            nome
        FROM {{ ref('raw_funcionarios') }}
    ),

    setor AS (
        SELECT
            SAFE_CAST(REGEXP_REPLACE(id_setor, r'\.0$', '') AS INT64) AS id_setor,
            SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS setor_nome
        FROM {{ ref('raw_setor') }}
    ),

    secretaria AS (
        SELECT
            SAFE_CAST(REGEXP_REPLACE(cd_ua, r'\.0$', '') AS INT64) AS id_secretaria,
            SAFE_CAST(REGEXP_REPLACE(sigla_ua, r'\.0$', '') AS STRING) AS secretaria_sigla,
            SAFE_CAST(REGEXP_REPLACE(nome_ua, r'\.0$', '') AS STRING) AS secretaria_nome
        FROM {{ source('unidades_admin', 'orgaos') }}
    ),

    final AS (
        SELECT
            f.id_funcionario,
            f.cpf,
            f.nome,
            s.id_setor,
            s.setor_nome,
            sec.id_secretaria,
            sec.secretaria_sigla
        FROM funcionarios f
        LEFT JOIN setor s ON f.id_funcionario = s.id_setor
        LEFT JOIN secretaria sec ON s.id_setor = sec.id_secretaria
    )

SELECT * FROM final
