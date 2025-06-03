

SELECT
  SAFE_CAST (id as STRING) as id_ranking_corrida,
  DATETIME (TIMESTAMP(createdAt)) as data_criacao,
  DATETIME (TIMESTAMP(updatedAt)) as data_atualizacao,
  SAFE_CAST (race as STRING) as id_corrida,  
  TO_JSON_STRING(competitors) as competidores,
  SAFE_CAST (ano_particao as INT64) as ano_particao,
  SAFE_CAST (mes_particao as INT64) as mes_particao,
  SAFE_CAST (dia_particao as INT64) as dia_particao,
  DATE(SAFE_CAST(ano_particao AS INT64), SAFE_CAST(mes_particao AS INT64), 1) AS data_particao
 
  
FROM
  {{ source('brutos_taxirio_staging','rankingraces') }}
