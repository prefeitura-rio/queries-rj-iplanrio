SELECT
  * EXCEPT(data_emissao_documento),
  
  -- Strings incompletas ('10/2021') viram NULL
  COALESCE(
    SAFE.PARSE_DATE('%Y-%m-%d', data_emissao_documento),
    SAFE.PARSE_DATE('%d/%m/%Y', data_emissao_documento)
  ) AS data_emissao_documento,

  LPAD(REGEXP_REPLACE(cnpj_emitente, r'[^0-9]', ''), 14, '0')
    AS cnpj_modelo_normalizado,
  SAFE_CAST(REGEXP_REPLACE(CAST(numero_documento AS STRING), r'[^0-9]', '') AS INT64)
    AS numero_nf_modelo_norm
FROM `rj-agent-cgm-triagem-nf.brutos_pipeline_extracao_staging.extracao_pagina`