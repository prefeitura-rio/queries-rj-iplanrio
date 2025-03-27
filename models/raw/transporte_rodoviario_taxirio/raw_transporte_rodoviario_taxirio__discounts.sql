SELECT
  SAFE_CAST (id as STRING) as id_desconto_associado,
  SAFE_CAST (description as STRING) as descricao,
  SAFE_CAST (value as FLOAT64) as desconto,
  SAFE_CAST (createdAt as DATE) as data_criacao,
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_staging.discounts`
