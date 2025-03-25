SELECT
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  DATETIME (TIMESTAMP(updatedAt)) as data_atualizacao,
  SAFE_CAST (id as STRING) as id_ranking_corrida,
  SAFE_CAST (race as STRING) as id_corrida,
  PARSE_JSON (competitors) AS competidores
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_homologacao_staging.rankingraces`
