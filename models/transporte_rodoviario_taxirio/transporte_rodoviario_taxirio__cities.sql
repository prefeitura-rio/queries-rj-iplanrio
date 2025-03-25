SELECT
  SAFE_CAST (id as STRING) as id_municipio,
  SAFE_CAST (name as STRING) as nome_municipio,
  SAFE_CAST (stateAbbreviation as STRING) as abrev_municipio,
  SAFE_CAST (isAbleToUsePaymentInApp as BOOL) as pode_usar_pagamento_app,
  SAFE_CAST (isCalulatedInApp as BOOL) as calculado_no_app,
  SAFE_CAST (loginLabel as STRING) as forma_de_login,
  PARSE_JSON (serviceStations) as estacoes_de_servico,
FROM
  `rj-iplanrio.transporte_rodoviario_taxirio_staging.cities`
