SELECT
  * EXCEPT(
    pagina,
    valor_documento,
    data_emissao_documento,
    timestamp_geracao
  ),

  -- 1. Cast numérico seguro: funciona direto se for INT64 ou se for STRING (com ou sem espaços)
  SAFE_CAST(pagina AS INT64) AS pagina,

  -- 2. Cast monetário seguro: garante coerção para STRING antes de trocar vírgula por ponto
  SAFE_CAST(REGEXP_REPLACE(CAST(valor_documento AS STRING), r',', '.') AS FLOAT64) AS valor_documento,

  -- 3. Parse de data
  COALESCE(
    SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_REPLACE(TRIM(CAST(data_emissao_documento AS STRING)), r'[\.\-]', '/')),
    SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(data_emissao_documento AS STRING)))
  ) AS data_emissao_documento,

  -- 4. Cast de timestamp
  SAFE_CAST(timestamp_geracao AS TIMESTAMP) AS timestamp_geracao,

  -- 5. Normalizações de chaves
  LPAD(REGEXP_REPLACE(cnpj_emitente, r'[^0-9]', ''), 14, '0')
    AS cnpj_modelo_normalizado,
  SAFE_CAST(REGEXP_REPLACE(CAST(numero_documento AS STRING), r'[^0-9]', '') AS INT64)
    AS numero_nf_modelo_norm

FROM `rj-agent-cgm-triagem-nf.brutos_pipeline_extracao_staging.extracao_pagina`
WHERE COALESCE(pipeline_status, 'ok') = 'ok'

-- 6. Deduplicação defensiva sem TRIM na coluna numérica
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY nome_arquivo, SAFE_CAST(pagina AS INT64)
  ORDER BY 
    CASE WHEN numero_documento IS NOT NULL THEN 1 ELSE 2 END,
    data_geracao DESC
) = 1