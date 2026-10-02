SELECT
  * EXCEPT(
    pagina,
    valor_documento,
    data_emissao_documento,
    confianca_extracao,
    timestamp_geracao
  ),

  SAFE_CAST(TRIM(pagina) AS INT64) AS pagina,
  SAFE_CAST(REGEXP_REPLACE(TRIM(valor_documento), r',', '.') AS FLOAT64) AS valor_documento,

  -- Datas incompletas como MM/AAAA viram NULL
  COALESCE(
    SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_REPLACE(TRIM(data_emissao_documento), r'[\.\-]', '/')),
    SAFE.PARSE_DATE('%Y-%m-%d', TRIM(data_emissao_documento))
  ) AS data_emissao_documento,

  SAFE_CAST(REGEXP_REPLACE(TRIM(confianca_extracao), r',', '.') AS FLOAT64) AS confianca_extracao,

  SAFE_CAST(timestamp_geracao AS TIMESTAMP) AS timestamp_geracao,

  LPAD(REGEXP_REPLACE(cnpj_emitente, r'[^0-9]', ''), 14, '0')
    AS cnpj_modelo_normalizado,
  SAFE_CAST(REGEXP_REPLACE(CAST(numero_documento AS STRING), r'[^0-9]', '') AS INT64)
    AS numero_nf_modelo_norm

FROM `rj-agent-cgm-triagem-nf.brutos_pipeline_extracao_staging.extracao_pagina`
WHERE COALESCE(pipeline_status, 'ok') = 'ok'

-- Elimina duplicatas de reprocessamento mantendo a execução mais recente
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY nome_arquivo, SAFE_CAST(TRIM(pagina) AS INT64)
  ORDER BY 
    CASE WHEN numero_documento IS NOT NULL THEN 1 ELSE 2 END,
    data_geracao DESC,
    id_origem_pipeline DESC
) = 1