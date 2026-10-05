-- Falha se houver multiplicação de linhas no Mart
WITH despesas_esperadas AS (
  SELECT od.id_documento
  FROM {{ ref('core_cgm_triagem_nf__osinfo_despesas') }} od
  WHERE od.nome_arquivo IN (
    SELECT DISTINCT nome_arquivo
    FROM {{ ref('core_cgm_triagem_nf__extracao_pagina') }}
  )
),

despesas_mart AS (
  SELECT
    id_documento,
    COUNT(*) AS qtd_linhas_mart
  FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
  GROUP BY id_documento
)

SELECT
  COALESCE(e.id_documento, m.id_documento) AS id_documento,
  e.id_documento AS id_documento_esperado,
  m.id_documento AS id_documento_mart,
  m.qtd_linhas_mart
FROM despesas_esperadas e
FULL OUTER JOIN despesas_mart m
  ON e.id_documento = m.id_documento
WHERE
  m.qtd_linhas_mart > 1      -- Duplicação / Fan-out
  OR m.id_documento IS NULL  -- Despesa esperada ausente no Mart
  OR e.id_documento IS NULL  -- Despesa extra inexistente na origem