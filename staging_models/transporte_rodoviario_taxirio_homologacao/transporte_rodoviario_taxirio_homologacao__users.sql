SELECT
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  SAFE_CAST (id as STRING) as id_usuario,
  SAFE_CAST (federalRevenueData_sex as STRING) as sexo,
  DATE(TIMESTAMP(birthDate)) as data_nascimento,
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_homologacao_staging.users`
