WITH mart_contagem AS (
  SELECT
    id_documento
  FROM {{ ref('mart_cgm_triagem_nf__despesas_classificada') }}
  GROUP BY id_documento
),

match_esperado AS (
  SELECT
    id_documento
  FROM {{ ref('core_cgm_triagem_nf__extracao_ia_match') }}
  GROUP BY id_documento
)

SELECT
  COALESCE(m.id_documento, e.id_documento) AS id_documento,
  CASE
    WHEN m.id_documento IS NULL THEN 'id_documento presente no match mas ausente no Mart'
    WHEN e.id_documento IS NULL THEN 'id_documento presente no Mart mas ausente no match'
  END AS motivo_falha
FROM mart_contagem m
FULL OUTER JOIN match_esperado e
  ON m.id_documento = e.id_documento
WHERE
  m.id_documento IS NULL
  OR e.id_documento IS NULL